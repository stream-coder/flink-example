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
public class TestCountTrigger extends Trigger<TransLog, TimeWindow> {
    private static final Logger log = LoggerFactory.getLogger(TestCountTrigger.class);
    private static final int TRIGGER_COUNT = 1;
    private int count = 0;

    @Override
    public TriggerResult onElement(TransLog element, long timestamp, TimeWindow window, TriggerContext ctx)
        throws Exception {
        if (count >= TRIGGER_COUNT) {
            count = 0;
            log.warn("count fire: {}", Util.format(element.getTransDate().getTime()));
            return TriggerResult.FIRE;
        } else {
            count++;
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }
}
