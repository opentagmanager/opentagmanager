export interface RateLimitKeyParts {
  publicKeyId: string;
  ip: string | null;
}

export interface RateLimitDecision {
  ok: boolean;
  remaining: number;
  resetMs: number;
}

export interface RateLimiter {
  take(key: RateLimitKeyParts): Promise<RateLimitDecision>;
  dispose?(): void | Promise<void>;
}

/**
 * Simple async mutex for serializing access to shared state
 * Prevents race conditions in the token bucket algorithm
 */
class AsyncMutex {
  private promise: Promise<void> = Promise.resolve();

  async acquire(): Promise<() => void> {
    const releasePromise = this.promise;
    let releaseFn: () => void;
    
    this.promise = new Promise((resolve) => {
      releaseFn = () => {
        resolve();
      };
    });
    
    await releasePromise;
    return releaseFn;
  }
}

export function buildRateKey(k: RateLimitKeyParts): string {
  const ip = k.ip ?? "unknown";
  return `${k.publicKeyId}:${ip}`;
}

export function createNoopLimiter(): RateLimiter {
  return {
    async take() {
      const now = Date.now();
      return {
        ok: true,
        remaining: Number.POSITIVE_INFINITY,
        resetMs: now + 1000,
      };
    },
  };
}

export function createInMemoryLimiter(opts: {
  rps: number;
  burst?: number;
  clock?: () => number;
  sweepIntervalMs?: number;
  idleTtlMs?: number;
}): RateLimiter {
  const rps = Math.max(1, Math.floor(opts.rps));
  const capacity = Math.max(1, Math.floor(opts.burst ?? rps));
  const now = opts.clock ?? (() => Date.now());
  const tokensPerMs = rps / 1000;

  type Bucket = {
    tokens: number;
    updatedAt: number;
    resetMs: number;
    lastHit: number;
  };
  const buckets = new Map<string, Bucket>();
  
  // Mutex per bucket to prevent race conditions on token operations
  const locks = new Map<string, AsyncMutex>();

  function getOrCreateMutex(key: string): AsyncMutex {
    let mutex = locks.get(key);
    if (!mutex) {
      mutex = new AsyncMutex();
      locks.set(key, mutex);
    }
    return mutex;
  }

  function refill(b: Bucket, t: number) {
    if (t <= b.updatedAt) return;
    const elapsed = t - b.updatedAt;
    b.tokens = Math.min(capacity, b.tokens + elapsed * tokensPerMs);
    b.updatedAt = t;
  }

  const idleTtlMs = opts.idleTtlMs ?? 5 * 60_000;
  const sweepEvery = opts.sweepIntervalMs ?? 60_000;
  const timer = setInterval(() => {
    const cutoff = now() - idleTtlMs;
    for (const [k, b] of buckets) {
      if (b.lastHit < cutoff) {
        buckets.delete(k);
        locks.delete(k); // Also clean up the mutex
      }
    }
  }, sweepEvery).unref?.();

  async function take(key: RateLimitKeyParts): Promise<RateLimitDecision> {
    const k = buildRateKey(key);
    const mutex = getOrCreateMutex(k);
    const release = await mutex.acquire();
    
    try {
      const t = now();
      let b = buckets.get(k);
      if (!b) {
        b = { tokens: capacity, updatedAt: t, resetMs: t, lastHit: t };
        buckets.set(k, b);
      }
      refill(b, t);
      b.lastHit = t;

      if (b.tokens >= 1) {
        b.tokens -= 1;
        const need = capacity - b.tokens;
        b.resetMs = t + Math.ceil(need / tokensPerMs);
        return { ok: true, remaining: Math.floor(b.tokens), resetMs: b.resetMs };
      } else {
        const waitMs = Math.ceil((1 - b.tokens) / tokensPerMs);
        b.resetMs = t + waitMs;
        return { ok: false, remaining: 0, resetMs: b.resetMs };
      }
    } finally {
      release();
    }
  }

  async function dispose() {
    if (timer) clearInterval(timer as any);
    buckets.clear();
    locks.clear();
  }

  return { take, dispose };
}
