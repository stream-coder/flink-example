package cn.com.jerry.flink.example.usecase.watermark;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.jerry.flink.example.common.pojo.TransLog;

/**
 * @author GangW
 */
public class TestEventTimeTrigger extends Trigger<TransLog, TimeWindow> {
    private static final Logger log = LoggerFactory.getLogger(TestEventTimeTrigger.class);
    private static final int TRIGGER_COUNT = 4;
    private int count = 0;

    @Override
    public TriggerResult onElement(TransLog element, long timestamp, TimeWindow window, TriggerContext ctx)
        throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            log.info("on element fire: {}", Util.format(window.maxTimestamp()));
            return TriggerResult.FIRE;
        } else {
            log.info("register event time timer: {}", Util.format(window.maxTimestamp()));
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        if (time == window.maxTimestamp()) {
            log.info("on event time fire: {}", Util.format(window.maxTimestamp()));
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
