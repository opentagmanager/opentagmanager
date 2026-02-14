import { Kafka, logLevel, type Producer } from "kafkajs";
import { KafkaUnavailableError } from "./errors";
import type { NormalizedEvent } from "./types";
import { env } from "@otm/env";
import { logger } from "./logger";
import { traceScope } from "./trace";

const {
  KAFKA_BROKERS,
  KAFKA_SASL_USERNAME,
  KAFKA_SASL_PASSWORD,
  KAFKA_SASL_MECHANISM,
  KAFKA_SASL_ENABLED,
  KAFKA_TOPIC_INGEST,
  KAFKA_CLIENT_ID,
  KAFKA_SSL,
} = process.env;

let producer: Producer | null = null;
let initializing: Promise<Producer> | null = null;

export async function getKafkaProducer(): Promise<Producer> {
  // Fast path: return existing producer
  if (producer) return producer;
  
  // Another request is already initializing, wait for it
  if (initializing) return initializing;

  if (!KAFKA_BROKERS || !KAFKA_TOPIC_INGEST) {
    throw new KafkaUnavailableError("Kafka configuration is missing", {
      detail: {
        KAFKA_BROKERS: env.KAFKA_BROKERS,
        KAFKA_TOPIC_INGEST: env.KAFKA_TOPIC_INGEST,
      },
    });
  }

  const saslEnabled = KAFKA_SASL_ENABLED === "true";

  // Create initialization promise to prevent concurrent initialization
  initializing = (async () => {
    try {
      const kafka = new Kafka({
        clientId: KAFKA_CLIENT_ID,
        brokers: KAFKA_BROKERS.split(","),
        ssl: KAFKA_SSL === "true",
        ...(saslEnabled
          ? {
              sasl: {
                mechanism: (KAFKA_SASL_MECHANISM ?? "scram-sha-512") as any,
                username: KAFKA_SASL_USERNAME ?? "",
                password: KAFKA_SASL_PASSWORD ?? "",
              },
            }
          : {}),
        logLevel: logLevel.ERROR,
      });

      const newProducer = kafka.producer();
      await newProducer.connect();
      producer = newProducer;
      return newProducer;
    } catch (err) {
      throw new KafkaUnavailableError("Failed to initialize Kafka producer", {
        cause: err,
      });
    } finally {
      // Clear the initializing flag so future calls check the producer again
      initializing = null;
    }
  })();

  return initializing;
}

export async function sendBatchToKafka(
  events: NormalizedEvent[],
  traceId?: string
): Promise<void> {
  if (!events.length) return;

  const topic = KAFKA_TOPIC_INGEST!;
  const producer = await getKafkaProducer();

  try {
    logger.debug("Kafka send", { topic, brokers: KAFKA_BROKERS });

    await producer.send({
      topic,
      messages: events.map((event) => ({
        key: event.projectId,
        value: JSON.stringify(event),
      })),
    });
    logger.info(
      "Enqueued events to Kafka",
      traceScope(traceId ?? "no-trace", {
        topic,
        count: events.length,
      })
    );
  } catch (err) {
    throw new KafkaUnavailableError("Failed to send batch to Kafka", {
      cause: err,
      detail: { topic, count: events.length },
    });
  }
}

export async function disconnectKafkaProducer(): Promise<void> {
  if (producer) {
    try {
      await producer.disconnect();
    } finally {
      producer = null;
    }
  }
}
