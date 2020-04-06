package cn.com.jerry.flink.example.usecase.delay.call;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author GangW
 */
public class DelayCall {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.addSource(new SourceFunction<ServerMsg>() {
            private long delay = TimeUnit.SECONDS.toMillis(1);
            private Random random = new Random();

            @Override
            public void run(SourceContext<ServerMsg> ctx) {
                SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
                for (int i = 0; i < 100; i++) {
                    int id = random.nextInt(10);
                    long time = System.currentTimeMillis();
                    ServerMsg msg = new ServerMsg("1", true, time);

                    ctx.collect(msg);

                    System.out.println(sdf.format(new Date(time)) + ", sleep " + id + " seconds.");
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(id));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void cancel() {}
        }).keyBy(ServerMsg::getServerId).process(new MonitorKeyedProcessFunction());

        env.execute("delay-monitor");
    }
}
