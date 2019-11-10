package cn.com.jerry.flink.java.connector.kafka;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ø
 * 
 * @author GangW
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Metric {
    public String name;

    public long timestamp;

    public Map<String, Object> fields;

    public Map<String, String> tags;
}
