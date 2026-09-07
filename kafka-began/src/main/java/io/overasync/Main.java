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

void main() {
    log.info("--Hello Kafka!--");

    final var properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVER, KAFKA_LOCAL);
    properties.setProperty("key.serializer", STRING_SERIALIZER);
    properties.setProperty("value.serializer", STRING_SERIALIZER);

    try (final var producer = new KafkaProducer<String, String>(properties)) {
        final var producerRecord = new ProducerRecord<String, String>("demo_java", "Hi Kafka!");
        producer.send(producerRecord);
    }

    log.info("--Everything done!--");
}