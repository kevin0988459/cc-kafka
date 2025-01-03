
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataProducerTest {

    private MockProducer<String, String> producer;

    @Before
    public void setUp() {
        producer = new MockProducer<>(
                true, new StringSerializer(), new StringSerializer());
    }

    /**
     * This test checks if the messages go to the correct topic and partition as
     * required. Additional test cases can be added by adding more entries to
     * test_trace and verifying here.
     *
     * @throws IOException
     */
    @Test
    public void testProducer() throws IOException {
        DataProducer dataProducer = new DataProducer(producer, "test_trace");

        dataProducer.sendData();

        List<ProducerRecord<String, String>> history = producer.history();

        List<ProducerRecord<String, String>> expected = Arrays.asList(
                new ProducerRecord<>("ad-click", null, null, "{\"blockId\":5648,\"type\":\"ENTERING_BLOCK\"}"),
                new ProducerRecord<>("ad-click", null, null, "{\"blockId\":5649,\"type\":\"DRIVER_LOCATION\"}"));

        Assert.assertEquals("Producer records not matched!", expected, history);
    }
}
