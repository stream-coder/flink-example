package cn.com.jerry.flink.example.common.pojo;

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
