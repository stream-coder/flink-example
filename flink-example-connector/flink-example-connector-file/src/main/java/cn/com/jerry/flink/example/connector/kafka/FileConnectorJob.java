package cn.com.jerry.flink.example.connector.kafka;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author GangW
 */
public class FileConnectorJob {
    public static void main(String[] args) throws Exception {
        Path pa = new Path("/home/master/qingshu");

        TextInputFormat format = new TextInputFormat(pa);

        BasicTypeInfo<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;

        format.setCharsetName("UTF-8");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource st = env.readFile(format, "/home/master/qingshu", FileProcessingMode.PROCESS_CONTINUOUSLY, 1L,
            (TypeInformation)typeInfo);

        st.print();

        env.execute();
    }
}
