package cn.com.jerry.flink.example.usecase.watermark;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.jerry.flink.example.common.pojo.TransLog;

/**
 * @author GangW
 */
public class JerryContinuousEventTimeTrigger extends Trigger<TransLog, TimeWindow> {
    private static final Logger log = LoggerFactory.getLogger(JerryContinuousEventTimeTrigger.class);

    private static final long serialVersionUID = 1L;

    private final long interval;

    /**
     * When merging we take the lowest of all fire timestamps as the new fire timestamp.
     */
    private final ReducingStateDescriptor<Long> stateDesc =
        new ReducingStateDescriptor<>("fire-time", new JerryContinuousEventTimeTrigger.Min(), LongSerializer.INSTANCE);

    private JerryContinuousEventTimeTrigger(long interval) {
        log.info("create {}", this.getClass().getSimpleName());
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(TransLog element, long timestamp, TimeWindow window, TriggerContext ctx)
        throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            log.info("on element fire: {}", Util.format(window.maxTimestamp()));
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        if (fireTimestamp.get() == null) {
            long start = timestamp - (timestamp % interval);
            long nextFireTimestamp = start + interval;
            ctx.registerEventTimeTimer(nextFireTimestamp);
            fireTimestamp.add(nextFireTimestamp);
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()) {
            log.info("on event time fire: {}", Util.format(window.maxTimestamp()));
            return TriggerResult.FIRE;
        }

        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);

        Long fireTimestamp = fireTimestampState.get();

        if (fireTimestamp != null && fireTimestamp == time) {
            log.info("on event time state fire: {}", Util.format(window.maxTimestamp()));
            fireTimestampState.clear();
            fireTimestampState.add(time + interval);
            ctx.registerEventTimeTimer(time + interval);
            return TriggerResult.FIRE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long timestamp = fireTimestamp.get();
        if (timestamp != null) {
            ctx.deleteEventTimeTimer(timestamp);
            fireTimestamp.clear();
        }
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(stateDesc);
        Long nextFireTimestamp = ctx.getPartitionedState(stateDesc).get();
        if (nextFireTimestamp != null) {
            ctx.registerEventTimeTimer(nextFireTimestamp);
        }
    }

    @Override
    public String toString() {
        return "ContinuousEventTimeTrigger(" + interval + ")";
    }

    @VisibleForTesting
    public long getInterval() {
        return interval;
    }

    /**
     * Creates a trigger that continuously fires based on the given interval.
     *
     * @param interval The time interval at which to fire.
     */
    public static JerryContinuousEventTimeTrigger of(Time interval) {
        return new JerryContinuousEventTimeTrigger(interval.toMilliseconds());
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }
}
