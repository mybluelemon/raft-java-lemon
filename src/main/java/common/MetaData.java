package common;

import lombok.Data;

@Data
public class MetaData {
    long currentTerm ;
    String votedFor ;
    long lastAppliedIndex;
    long commitIndex ;
}
