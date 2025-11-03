import json
from kafka import KafkaProducer, KafkaConsumer
from loguru import logger
from etl.utils.utils import load_config

config = load_config()


# Initialize Kafka Producer
def init_kafka():
    logger.info("Initializing Kafka producer...")

    try:
        producer = KafkaProducer(
            bootstrap_servers=config["kafka"]["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        logger.success("Kafka producer initialized")
        return producer
    except Exception as e:
        logger.exception(f"Error during Kafka producer initialization: {e}")


def send_to_kafka(producer, topic, data):
    logger.info("Sending data to Kafka...")

    try:
        for item in data:
            producer.send(topic, value=item)
        producer.flush()
        logger.success(f"Data sent to Kafka")

    except Exception as e:
        logger.exception(f"Error during sending the data to Kafka: {e}")


# -- Kafka Consumer helpers -------------------------------------------------
def init_kafka_consumer(
    topic,
    group_id=None,
    bootstrap_servers=None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m is not None else None,
):
    """Initialize and return a KafkaConsumer subscribed to `topic`.

    Parameters:
    - topic: topic name (str) or list of topics
    - group_id: consumer group id
    - bootstrap_servers: list or comma-separated string; if None, taken from `config`
    - auto_offset_reset: 'earliest' or 'latest'
    - enable_auto_commit: whether to auto-commit offsets
    - value_deserializer: function to deserialize message value bytes -> python object

    Returns: KafkaConsumer instance
    """
    logger.info(f"Initializing Kafka consumer for topic={topic} group_id={group_id}...")
    try:
        if bootstrap_servers is None:
            bootstrap_servers = config["kafka"]["bootstrap_servers"]

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            value_deserializer=value_deserializer,
        )

        logger.success("Kafka consumer initialized")
        return consumer
    except Exception as e:
        logger.exception(f"Error during Kafka consumer initialization: {e}")
        raise


def get_messages_from_consumer(consumer, timeout_ms=1000, max_messages=1):
    """Poll the consumer and return up to `max_messages` message values.

    - consumer: KafkaConsumer instance
    - timeout_ms: poll timeout in milliseconds
    - max_messages: maximum number of messages to return

    Returns: list of message values (deserialized)
    """
    messages = []
    try:
        records = consumer.poll(timeout_ms=timeout_ms)
        for tp, rec_list in records.items():
            for rec in rec_list:
                messages.append(rec.value)
                if len(messages) >= max_messages:
                    break
            if len(messages) >= max_messages:
                break

        return messages
    except Exception as e:
        logger.exception(f"Error while polling consumer: {e}")
        return messages


def fetch_one_message_from_topic(topic, timeout_seconds=5, group_id=None, bootstrap_servers=None):
    """Convenience function: create a consumer, fetch one message (or None), then close it.

    Returns the deserialized message value or None if no message arrived within timeout.
    """
    consumer = None
    try:
        consumer = init_kafka_consumer(
            topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="latest",
            enable_auto_commit=False,
        )

        msgs = get_messages_from_consumer(consumer, timeout_ms=int(timeout_seconds * 1000), max_messages=1)
        return msgs[0] if msgs else None
    except Exception as e:
        logger.exception(f"Error fetching message from topic {topic}: {e}")
        return None
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass
