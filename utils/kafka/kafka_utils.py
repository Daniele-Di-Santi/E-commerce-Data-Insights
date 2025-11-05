import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from loguru import logger
from utils.utils import load_config

config = load_config()


# -- Kafka Producer helpers -------------------------------------------------
def init_kafka_producer():
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

def send_to_kafka(topic, data, producer=None):
    if producer is None:
        producer = init_kafka_producer()

    logger.info("Sending data to Kafka...")
    try:
        for item in data:
            producer.send(topic, value=item)
        producer.flush()
        logger.success(f"Data (len: {len(data)}) sent to Kafka in topic: {topic}")

    except Exception as e:
        logger.exception(f"Error during sending the data to Kafka: {e}")
    
    finally:
        producer.close()


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

def get_messages_from_consumer(consumer=None, topic=None, group_id=None, timeout_ms=1000, max_messages=None):
    """Poll the consumer and return up to `max_messages` message values.

    - consumer: KafkaConsumer instance
    - timeout_ms: poll timeout in milliseconds
    - max_messages: maximum number of messages to return

    Returns: list of message values (deserialized)
    """
    if consumer is None:
        if topic is None:
            raise ValueError("Either consumer or topic must be provided")
        
        consumer = init_kafka_consumer(topic, group_id=group_id)
    
    if max_messages is None:
        max_messages = float('inf')

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
    
    finally:
        consumer.close()


def force_clean_kafka_topic(topic: str, partitions: int = 1, replication_factor: int = 1):
    """Pulisce un topic Kafka eliminandolo e ricreandolo.
    Questo è il modo più sicuro per assicurarsi che tutti i messaggi vengano rimossi."""
    
    logger.info(f"Eliminazione e ricreazione topic Kafka: {topic}")
    
    # Crea admin client
    admin_client = KafkaAdminClient(
        bootstrap_servers=config["kafka"]["bootstrap_servers"]
    )
    
    try:
        # Elimina il topic se esiste
        try:
            admin_client.delete_topics([topic])
            logger.info(f"Topic {topic} eliminato")
        except UnknownTopicOrPartitionError:
            logger.info(f"Topic {topic} non esisteva")
    
        # Aspetta un momento per assicurarsi che l'eliminazione sia completata
        import time
        time.sleep(2)
        
        # Ricrea il topic
        new_topic = NewTopic(
            name=topic,
            num_partitions=partitions,
            replication_factor=replication_factor
        )
        
        admin_client.create_topics([new_topic])
        logger.success(f"Topic {topic} ricreato con successo")
        
    except Exception as e:
        logger.exception(f"Errore durante la pulizia forzata del topic: {e}")
        raise
    
    finally:
        admin_client.close()

def reset_consumer_offsets(topic: str, group_id: str):
    """Resetta gli offset di un consumer group a latest."""
    logger.info(f"Reset offset per gruppo {group_id} su topic {topic}")
    
    try:
        # Crea un consumer nel gruppo
        consumer = init_kafka_consumer(
            topic,
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True
        )
        
        # Forza la lettura e il commit degli offset latest
        consumer.poll(timeout_ms=1000)
        consumer.commit()
        consumer.close()
        
        logger.success(f"Offset resettati per topic={topic} group={group_id}")
        
    except Exception as e:
        logger.exception(f"Errore durante il reset degli offset: {e}")
        raise


