package cn.com.jerry.flink.java.wordcount.flink.java.timerOutput;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.com.jerry.flink.java.wordcount.flink.pojo.TransLog;

/**
 * @author GangW
 */
public class TimerOutputApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    }

    private List<TransLog> genLogs() {
        List<TransLog> logList = new ArrayList<>();

        return logList;
    }
}
