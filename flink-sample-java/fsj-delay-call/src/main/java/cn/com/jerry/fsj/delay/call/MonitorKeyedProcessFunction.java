package cn.com.jerry.fsj.delay.call;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 当服务器连续5分钟无反应时，输入告警。之后如果一直无信息，也每隔5分钟输出告警
 * 
 * @author GangW
 */
public class MonitorKeyedProcessFunction extends KeyedProcessFunction<String, ServerMsg, String> {
    private ValueState<Long> timeState;
    private ValueState<Long> serverTimeState;
    private ValueState<String> serverState;

    private static final long TIME_DELAY = TimeUnit.SECONDS.toMillis(5);

    @Override
    public void open(Configuration parameters) {
        timeState = getRuntimeContext().getState(new ValueStateDescriptor<>("timeState", Long.class));
        serverTimeState = getRuntimeContext().getState(new ValueStateDescriptor<>("serverTime", Long.class));
        serverState = getRuntimeContext().getState(new ValueStateDescriptor<>("serverState", String.class));
    }

    /**
     * 每隔消息来，都要注册一个5分钟后的监控定时器。有两点优化：</br>
     * <ul>
     * <li>注册新的定时器之前，删除老的定时器</li>
     * <li>如果收到的是迟到的消息，不会更新定时器</li>
     * </ul>
     */
    @Override
    public void processElement(ServerMsg value, Context ctx, Collector<String> out) throws Exception {
        if (timeState.value() != null) {
            long st = serverTimeState.value();
            /* 新来的数据比之前的数据更晚 */
            if (st < value.getTimestamp()) {
                long time = timeState.value();
                ctx.timerService().deleteProcessingTimeTimer(time);

                long monitorTime = ctx.timerService().currentProcessingTime() + TIME_DELAY;
                timeState.update(monitorTime);
                serverTimeState.update(value.getTimestamp());
                ctx.timerService().registerProcessingTimeTimer(monitorTime);
            }
        } else {
            long monitorTime = ctx.timerService().currentProcessingTime() + TIME_DELAY;
            timeState.update(monitorTime);
            serverTimeState.update(value.getTimestamp());
            // 只需要第一次的时候更新就行
            serverState.update(value.getServerId());

            ctx.timerService().registerProcessingTimeTimer(monitorTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (timestamp == timeState.value()) {
            long newMonitorTime = timestamp + TIME_DELAY;
            timeState.update(newMonitorTime);
            ctx.timerService().registerProcessingTimeTimer(newMonitorTime);

            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
            String msg =
                String.format("%s WARN! server [ %s ] may be shutdown, please restart. last heartbeat time is %s",
                    sdf.format(new Date()), serverState.value(), sdf.format(new Date(serverTimeState.value())));

            System.out.println(msg);
        }
    }
}
