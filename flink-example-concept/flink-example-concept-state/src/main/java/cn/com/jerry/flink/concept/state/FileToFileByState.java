package cn.com.jerry.flink.concept.state;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 通过使用状态实现精准的文件拷贝
 * 
 * @author GangW
 */
public class FileToFileByState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启检查点机制，并指定状态检查点之间的时间间隔
        env.enableCheckpointing(1000);

        // 其他可选配置如下：
        // 设置语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置执行Checkpoint操作时的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置最大并发执行的检查点的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 将检查点持久化到外部存储
        env.getCheckpointConfig()
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果有更近的保存点时，是否将作业回退到该检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // 配置 FsStateBackend
        env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));
        // 配置 RocksDBStateBackend
        env.setStateBackend(new RocksDBStateBackend("hdfs://namenode:40010/flink/checkpoints"));

    }
}
