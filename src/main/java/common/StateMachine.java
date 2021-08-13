package common;

import Model.LogEntry;

public interface StateMachine {

    void apply(LogEntry logEntry);

    LogEntry get(String key);

    String getString(String key);

    void setString(String key, String value);

    void delString(String... key);
}
