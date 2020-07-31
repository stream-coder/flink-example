package cn.com.jerry.flink.example.usecase.watermark;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author GangW
 */
public class Util {
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public synchronized static String format(long time) {
        return SDF.format(new Date(time));
    }
}
