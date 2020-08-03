package cn.com.jerry.flink.example.connector.kafka;

import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author GangW
 */
public class KafkaPauseRead {
    private static final Logger log = LoggerFactory.getLogger(KafkaPauseRead.class);

    private static String SOURCE_TOPIC = "trans-flow-sc";
    private static int RECORDS = 1000;

    private static int MAX_RECORDS = 3000;

    @Test
    public void test1() {
        plainReader();
    }

    @Test
    public void test2() {
        plainReader();
    }

    public static void plainReader() {
        Map<Integer, Long> map = new HashMap<>(16);

        Properties prop = properties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Collections.singletonList(SOURCE_TOPIC));

        for (int i = 0; i < 100000; i++) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            System.out.println(records.count());
            for (ConsumerRecord<String, String> r : records) {
                Long l = map.get(r.partition());
                if (l == null) {
                    map.put(r.partition(), 1L);
                } else {
                    map.put(r.partition(), l + 1);
                }
            }

            Set<Integer> paused = new HashSet<>();
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                if (entry.getValue() > MAX_RECORDS) {
                    paused.add(entry.getKey());
                }
            }

            Set<TopicPartition> partitions = consumer.assignment();
            log.info("{}", partitions);
            List<TopicPartition> pausedPartition = new ArrayList<>(paused.size());
            if (!paused.isEmpty()) {
                for (Integer pId : paused) {
                    for (TopicPartition topicPartition : partitions) {
                        if (topicPartition.partition() == pId) {
                            pausedPartition.add(topicPartition);
                        }
                    }
                }
                consumer.pause(pausedPartition);
            }
        }

        consumer.close();
    }

    public static Properties properties() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.100.2.180:9092");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "abc");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, RECORDS);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return prop;
    }
}
