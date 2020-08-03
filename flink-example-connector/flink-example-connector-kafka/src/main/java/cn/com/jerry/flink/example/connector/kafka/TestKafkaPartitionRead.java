package cn.com.jerry.flink.example.connector.kafka;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 看看在使用eventtime的时候，flink是如何管理不同分区的时间的
 * 
 * @author GangW
 */
public class TestKafkaPartitionRead {
    private static String SOURCE_TOPIC = "trans-flow-sc";

    public static void main(String[] args) throws Exception {
        flinkReader();
    }

    public static void flinkReader() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        Properties prop = properties();

        FlinkKafkaConsumer<Message> c =
            new FlinkKafkaConsumer<>(SOURCE_TOPIC, new TransFlowKafkaDeserializationSchema(), prop);

        env.addSource(c).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Message>() {
            private long time = Long.MIN_VALUE;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(time);
            }

            @Override
            public long extractTimestamp(Message element, long previousElementTimestamp) {
                time = Math.max(time, element.getTime());

                return time;
            }
        }).map(new MapFunction<Message, Message>() {
            private int lastPartition = -1;
            private int lastCount = 0;

            @Override
            public Message map(Message value) throws Exception {
                if (value.getPartition() == lastPartition) {
                    lastCount++;
                } else {
                    System.out.println(lastPartition + "\t" + lastCount);
                    lastPartition = value.getPartition();
                    lastCount = 1;
                }
                return value;
            }
        }).timeWindowAll(Time.minutes(10)).aggregate(new AggregateFunction<Message, Long, Long>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(Message value, Long accumulator) {
                return accumulator + 1L;
            }

            @Override
            public Long getResult(Long accumulator) {
                return accumulator;
            }

            @Override
            public Long merge(Long a, Long b) {
                return a + b;
            }
        }, new AllWindowFunction<Long, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Long> values, Collector<String> out) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                out.collect(String.format("from = %s, to = %s, count = %s", sdf.format(new Date(window.getStart())),
                    sdf.format(new Date(window.getEnd())), values.iterator().next()));
            }
        }).print();

        env.execute("asba");
    }

    public static Properties properties() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.100.2.180:9092");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10240);
        prop.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024000);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return prop;
    }

    static class TransFlowKafkaDeserializationSchema implements KafkaDeserializationSchema<Message> {
        @Override
        public boolean isEndOfStream(Message nextElement) {
            return false;
        }

        @Override
        public Message deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            Message messaage = new Message();
            messaage.setPartition(record.partition());
            messaage.setOffset(record.offset());
            String str = new String(record.value(), StandardCharsets.UTF_8);
            messaage.setTime(Long.parseLong(str.split(",")[0]));

            return messaage;
        }

        @Override
        public TypeInformation<Message> getProducedType() {
            return TypeInformation.of(Message.class);
        }
    }
}
