import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

static final Class<?> currentClass = MethodHandles.lookup().lookupClass();
static final Logger log = LoggerFactory.getLogger(currentClass.getSimpleName());
static final String BOOTSTRAP_SERVER = "bootstrap.servers";
static final String KAFKA_LOCAL = "127.0.0.1:9092";
static final String STRING_DESERIALIZER = StringDeserializer.class.getName();
static final String TOPIC = "demo_jkafka";
static final String GROUP_ID = "the-jc-app";

static Properties getProperties() {
    final var properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVER, KAFKA_LOCAL);
    properties.setProperty("key.deserializer", STRING_DESERIALIZER);
    properties.setProperty("value.deserializer", STRING_DESERIALIZER);
    properties.setProperty("group.id", GROUP_ID);
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return properties;
}

void main() {
    log.info("--Kafka Consumer--");
    final var properties = getProperties();

    try (final var consumer = new KafkaConsumer<String, String>(properties)) {
        final var mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown! So, calling the consumer.wakeup method!");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                mainThread.interrupt();
            }
        }));

        consumer.subscribe(List.of(TOPIC));

        try {
            while (true) {
                log.info("Polling records...");
                final var consumerRecords = consumer.poll(Duration.ofSeconds(1));
                consumerRecords.forEach(r -> {
                    log.info("""
                            
                            Key:       {} | Value:  {},
                            Partition: {} | Offset: {}
                            """, r.key(), r.value(), r.partition(), r.offset());
                });
            }
        } catch (WakeupException _) {
            log.info("Consumer is going to shutdown!");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        }
        log.info("Consumer gracefully shutdown!");
    }
}
