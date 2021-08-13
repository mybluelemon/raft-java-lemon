package log;

import Model.LogEntry;
import common.MetaData;

/**
 *日志系统，不想自己写，用index 作为key ,entry 作为value
 * 我们一般需要得到最后的index，她的term,和index
 * db会保存额外控制信息last_index
 */
public interface LogSysModule {

    void write(LogEntry logEntry);

    LogEntry read(Long index);

    void removeOnStartIndex(Long startIndex);

    LogEntry getLast();

    Long getLastIndex();

    MetaData getMetaData();

    void setMetaData(long currentTerm , String votedFor ,   long lastAppliedIndex, long commitIndex );
}
