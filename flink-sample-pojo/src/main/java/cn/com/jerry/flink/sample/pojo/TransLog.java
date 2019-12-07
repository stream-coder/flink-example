package cn.com.jerry.flink.sample.pojo;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author GangW
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransLog {
    private String from;
    private String to;
    private Date transDate;

    private long transAmt;
    private String transCode;
}
