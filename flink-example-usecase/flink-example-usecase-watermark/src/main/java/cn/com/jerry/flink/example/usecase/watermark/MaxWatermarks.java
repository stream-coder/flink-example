package cn.com.jerry.flink.example.usecase.watermark;

import javax.annotation.Nullable;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.jerry.flink.example.common.pojo.TransLog;

/**
 * @author GangW
 */
public class MaxWatermarks implements AssignerWithPeriodicWatermarks<TransLog> {
    private static final Logger log = LoggerFactory.getLogger(MaxWatermarks.class);
    private long max = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(max);
    }

    @Override
    public long extractTimestamp(TransLog element, long previousElementTimestamp) {
        max = Math.max(max, element.getTransDate().getTime());
        return max;
    }
}
