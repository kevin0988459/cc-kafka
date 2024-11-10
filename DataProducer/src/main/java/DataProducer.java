
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DataProducer {

    private Producer<String, String> producer;
    private String traceFileName;
    private static final String EVENTS_TOPIC = "ad-click";
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
                ProducerRecord<String, String> record = new ProducerRecord<>(EVENTS_TOPIC, null, line);
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
