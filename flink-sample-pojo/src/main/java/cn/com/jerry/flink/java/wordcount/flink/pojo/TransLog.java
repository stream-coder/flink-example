package cn.com.jerry.flink.java.wordcount.flink.pojo;

import java.util.Date;

import lombok.Data;

/**
 * @author GangW
 */
@Data
public class TransLog {
    private String from;
    private String to;
    private Date transDate;

    private long transAmt;
    private String transCode;
}
