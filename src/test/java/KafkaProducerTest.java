import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaProducerTest {

    public static void main(String[] args) {

        // Set Kafka broker address and topic name
        String bootstrapServers = "localhost:9092";
        String topicName = "transactions";

        // Set properties for the Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Create a message
        String message = "Hello, Kafka!";

        // Create a ProducerRecord with the message and send it to the topic
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);

        // Send the message asynchronously
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("Message sent successfully! Topic: " + metadata.topic() +
                            ", Partition: " + metadata.partition() +
                            ", Offset: " + metadata.offset());
                } else {
                    System.err.println("Error while sending message: " + exception.getMessage());
                }
            }
        });

        // Close the producer
        producer.close();
    }
}
