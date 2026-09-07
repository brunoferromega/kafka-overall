import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

static final Class<?> currentClass = MethodHandles.lookup().lookupClass();
static final Logger log = LoggerFactory.getLogger(currentClass.getSimpleName());
static final String BOOTSTRAP_SERVER = "bootstrap.servers";
static final String KAFKA_LOCAL = "127.0.0.1:9092";
static final String STRING_SERIALIZER = StringSerializer.class.getName();
static final String TOPIC = "demo_jkafka";

static Properties getProperties() {
    final var properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVER, KAFKA_LOCAL);
    properties.setProperty("key.serializer", STRING_SERIALIZER);
    properties.setProperty("value.serializer", STRING_SERIALIZER);
    properties.setProperty("batch.size", "400");
//    properties.setProperty("partition.clas", RoundRobinPartitioner.class.getName());
    return properties;
}

void main() {
    log.info("--Hello Kafka!--");
    final var properties = getProperties();
    try (final var producer = new KafkaProducer<String, String>(properties)) {
        IntStream.range(0, 10).forEach(_ -> {
            IntStream.range(0, 30).forEach(i -> {
                final var producerRecord = new ProducerRecord<String, String>(TOPIC, "K Callback msg: " + i);
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) log.error("Something wrong had happened!", exception);
                    log.info("""
                            -------------
                            Topic:     {},
                            Partition: {},
                            Offset:    {},
                            Timestamp: {},
                            """, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                });
            });
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
    log.info("--Everything done!--");
}
