package cn.com.jerry.flink.java.connector.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.alibaba.fastjson.JSON;

/**
 * @author GangW
 */
public class KafkaUtils {
    public static final String BROKER_LIST = "localhost:9092";
    /**
     * kafka TOPIC，Flink 程序中需要和这个统一
     */
    public static final String TOPIC = "metric";
    public static final KafkaProducer<String, String> KAFKA_PRODUCER;
    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        // key 序列化
        props.put("key.serializer", StringSerializer.class.getName());
        // value 序列化
        props.put("value.serializer", StringSerializer.class.getName());
        KAFKA_PRODUCER = new KafkaProducer<>(props);
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println(StringSerializer.class.getName());
        try {
            while (true) {
                Thread.sleep(100);
                writeToKafka();
            }
        } finally {
            if (KAFKA_PRODUCER != null) {
                KAFKA_PRODUCER.close();
            }
        }
    }

    public static void writeToKafka() throws InterruptedException {
        Map<String, String> tags = new HashMap<>(2);
        tags.put("cluster", "zhisheng");
        tags.put("host_ip", "101.147.022.106");

        Map<String, Object> fields = new HashMap<>(4);
        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 27244873d);

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        metric.setTags(tags);
        metric.setFields(fields);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, null, JSON.toJSONString(metric));
        KAFKA_PRODUCER.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        KAFKA_PRODUCER.flush();
    }

}
