package cn.com.jerry.fsj.distinctOrderId;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author GangW
 */
public class KeyedStateDeduplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        // 使用 RocksDBStateBackend 做为状态后端，并开启增量 Checkpoint
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:///flink/checkpoints", true);
        rocksDBStateBackend.setNumberOfTransferingThreads(3);
        // 设置为机械硬盘+内存模式，强烈建议为 RocksDB 配备 SSD
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        rocksDBStateBackend.enableTtlCompactionFilter();
        env.setStateBackend(rocksDBStateBackend);

        // Checkpoint 间隔为 10 分钟
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(10));
        // 配置 Checkpoint
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(8));
        checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(20));
        checkpointConf
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Kafka Consumer 配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "keyed-state-deduplication");
        FlinkKafkaConsumerBase<String> kafkaConsumer =
            new FlinkKafkaConsumer011<>(DeduplicationExampleUtil.topic, new SimpleStringSchema(), props)
                .setStartFromGroupOffsets();

        env.addSource(kafkaConsumer).map(log -> GsonUtil.fromJson(log, UserVisitWebEvent.class)) // 反序列化 JSON
            .keyBy((KeySelector<UserVisitWebEvent, String>)UserVisitWebEvent::getId).addSink(new KeyedStateSink());

        env.execute("KeyedStateDeduplication");
    }

    // 用来维护实现百亿去重逻辑的算子
    public static class KeyedStateSink extends RichSinkFunction<UserVisitWebEvent> {
        // 使用该 ValueState 来标识当前 Key 是否之前存在过
        private ValueState<Boolean> isExist;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Boolean> keyedStateDuplicated =
                new ValueStateDescriptor<>("KeyedStateDeduplication", TypeInformation.of(new TypeHint<Boolean>() {}));
            // 状态 TTL 相关配置，过期时间设定为 36 小时
            StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Time.hours(36)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupInRocksdbCompactFilter(50000000L).build();
            // 开启 TTL
            keyedStateDuplicated.enableTimeToLive(ttlConfig);
            // 从状态后端恢复状态
            isExist = getRuntimeContext().getState(keyedStateDuplicated);
        }

        @Override
        public void invoke(UserVisitWebEvent value, Context context) throws Exception {
            // 当前 key 第一次出现时，isExist.value() 会返回 null
            // key 第一次出现，说明当前 key 在之前没有被处理过，
            // 此时应该执行正常处理代码的逻辑，并给状态 isExist 赋值，标识当前 key 已经处理过了，
            // 下次再有相同的主键 时，isExist.value() 就不会为 null 了
            if (null == isExist.value()) {
                // ... 这里执行代码处理的逻辑
                // 执行完处理逻辑后，更新状态值
                isExist.update(true);
            } else {
                // 如果 isExist.value() 不为 null，表示当前 key 在之前已经被处理过了，
                // 所以当前数据应该被过滤
            }
        }
    }
}
