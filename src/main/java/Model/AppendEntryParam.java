package Model;

import lombok.Data;

import java.util.List;

@Data
public class AppendEntryParam {
    /**
     * leader 在复制 ，或者心跳
     */
    /**
     * term 当前leader的term
     * leaderId (本leaderId ,只是为了其他node 转发)
     * prevLogIndex 想要复制的entries 的前面一个entry的index
     * prevLogTerm 想要复制的entries 的前面一个entry的term
     * entries[] 需要复制的log entries , heartBeat 时是null
     * leaderCommit -》 本leader的commitindex
     */

    long term;
    String leaderId;
    long prevLonIndex;
    long prevLogTerm;
    List<LogEntry> entries;
    long leaderCommit;
}
