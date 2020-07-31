package cn.com.jerry.flink.example.usecase.watermark;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.jerry.flink.example.common.pojo.TransLog;

/**
 * @author GangW
 */
public class LogSinkFunction extends RichSinkFunction<TransLog> {
    private static final AtomicLong SINK_COUNT = new AtomicLong();

    private static final Logger log = LoggerFactory.getLogger(LogSinkFunction.class);

    @Override
    public void invoke(TransLog value, Context context) throws Exception {
        SINK_COUNT.incrementAndGet();
        RuntimeContext runtimeContext = getRuntimeContext();
        log.info("sink -> key: {}, date: {}, value: {}, ts: {}, sinkCount={}", value.getFrom(),
            Util.format(value.getTransDate().getTime()), value.getTransAmt(), Util.format(context.timestamp()),
            SINK_COUNT.get());
    }
}
