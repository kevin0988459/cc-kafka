
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DataProducer {

    private Producer<String, String> producer;
    private String traceFileName;
    private static final String DRIVER_LOCATIONS_TOPIC = "driver-locations";
    private static final String EVENTS_TOPIC = "events";
    private static final int PARTITION_COUNT = 5;
    private JsonParser jsonParser;

    public DataProducer(Producer producer, String traceFileName) {
        this.producer = producer;
        this.traceFileName = traceFileName;
        this.jsonParser = new JsonParser();
    }

    public void sendData() {
        try (BufferedReader reader = new BufferedReader(new FileReader(traceFileName))) {
            String line;

            while ((line = reader.readLine()) != null) {
                // parse the json object
                JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();
                String type = jsonObject.get("type").getAsString();
                int blockId = jsonObject.get("blockId").getAsInt();
                // determine the topic
                String topic = type.equals("DRIVER_LOCATION") ? DRIVER_LOCATIONS_TOPIC : EVENTS_TOPIC;
                // determine the partition
                int partition = blockId % PARTITION_COUNT;
                // send the message
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, null, line);
                sendMessage(record);
            }

        } catch (Exception e) {
            System.err.println("Error reading trace file: " + e.getMessage());
        } finally {
            producer.close();
        }
    }

    /**
     * send a message to kafka
     *
     * @param record
     */
    private void sendMessage(ProducerRecord<String, String> record) {
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Sent record to topic %s partition %d offset %d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error sending record: " + e.getMessage());
        }
    }

}
