package cn.com.jerry.flink.example.usecase.state.backend.rocksdb;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author GangW
 */
public class RocksDbBackendSample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new RocksDBStateBackend("file:/Users/wg/tools/flink-1.9.1/rocksdb"));
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.fromElements(WORDS).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.toLowerCase().split("\\W+");

                for (String split : splits) {
                    if (split.length() > 0) {
                        out.collect(new Tuple2<>(split, 1));
                    }
                }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
                throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value1.f1);
            }
        }).print();
        // Streaming 程序必须加这个才能启动程序，否则不会有结果
        env.execute("word count streaming demo");
    }

    private static final String[] WORDS = new String[] {"To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer", "the bigger the dream, the more important the team"};
}
