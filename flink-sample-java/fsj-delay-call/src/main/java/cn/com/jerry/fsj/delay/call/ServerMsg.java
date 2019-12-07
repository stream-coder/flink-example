package cn.com.jerry.fsj.delay.call;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author GangW
 */
@Data
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class ServerMsg {
    private String serverId;
    private boolean isOnline;
    private long timestamp;
}
