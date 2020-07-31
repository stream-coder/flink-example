package cn.com.jerry.flink.example.connector.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author GangW
 */
public class PureKafkaToKafkaCheckpoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setCheckpoint(env);
        String sourceTopic = "trans-flow-sink";

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        FlinkKafkaConsumer<String> kafkaConsumer =
            new FlinkKafkaConsumer<>(sourceTopic, new SimpleStringSchema(), consumerProperties);

        DataStream<String> source = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (int i = 0; i < 300; i++) {
                    ctx.collect(i + "");
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {

            }
        }).name("source");

        String sinkTopic = "trans-flow-sink2";

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TimeUnit.MINUTES.toMillis(10) + "");

        AtomicLong count = new AtomicLong();

        FlinkKafkaProducer<String> producer =
            new FlinkKafkaProducer<>(sourceTopic, (String element, @Nullable Long timestamp) -> {
                System.out.println(count.incrementAndGet());
                return new ProducerRecord<>(sinkTopic, element.getBytes(StandardCharsets.UTF_8));
            }, producerProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE, 10);

        source.addSink(producer).name("sink");

        env.execute("string");
    }

    private static void setCheckpoint(StreamExecutionEnvironment env) {
        // 设置并行度,为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);

        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // allow job recovery fallback to checkpoint when there is a more recent savepoint
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // checkpoint的设置
        // 每隔10s进行启动一个检查点【设置checkpoint的周期】
        // env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        // // 确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // // 检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        // env.getCheckpointConfig().setCheckpointTimeout(10000);
        // // 同一时间只允许进行一次检查点
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // // 表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        // env.getCheckpointConfig()
        // .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // // 设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
        env.setStateBackend(new FsStateBackend("file:///Users/wg/cp/"));
        //
        // env.getCheckpointConfig().enableExternalizedCheckpoints(
        // org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    }
}
