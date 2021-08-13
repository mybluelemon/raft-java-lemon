package Model;

import lombok.Data;

@Data
public class AppendEntryRes {
    /** 当前的任期号，用于领导人去更新自己 */
    long term;

    /** 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真  */
    boolean success;
}
