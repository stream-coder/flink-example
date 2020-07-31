package cn.com.jerry.flink.example.usecase.watermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import cn.com.jerry.flink.example.common.pojo.TransLog;

/**
 * @author GangW
 */
public class WaterMarkCase {
    private static final int LOG_COUNT = 3;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<TransLog> source =
            env.fromCollection(transLogList()).assignTimestampsAndWatermarks(new MaxWatermarks());
        WindowedStream<TransLog, Tuple, TimeWindow> windowedStream =
            source.keyBy("from").timeWindow(Time.seconds(3), Time.seconds(1)).trigger(new TestCountTrigger());

        windowedStream.sum("transAmt").addSink(new LogSinkFunction());

        env.execute();
    }

    private static List<TransLog> transLogList() throws ParseException {
        List<TransLog> logs = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        long start = sdf.parse("20200506211010555").getTime();

        for (int i = 0; i < LOG_COUNT; i++) {
            TransLog log = new TransLog();
            log.setFrom("abc");
            log.setTransDate(new Date(start + 1_000 * i));
            log.setTransAmt(i);

            logs.add(log);
        }

        return logs;
    }
}
