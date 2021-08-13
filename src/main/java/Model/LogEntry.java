package Model;

import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

@Data
public class LogEntry implements Serializable, Comparable{

    private long index;

    private long term;

    private Command command;

    @Override
    public int compareTo(Object o) {
        if (o == null) {
            return -1;
        }
        if (this.getIndex() > ((LogEntry) o).getIndex()) {
            return 1;
        }
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
                Objects.equals(index, logEntry.index) &&
                Objects.equals(command, logEntry.command);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, term, command);
    }
}
