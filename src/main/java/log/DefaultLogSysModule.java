package log;

import Model.LogEntry;
import com.alibaba.fastjson.JSON;
import common.MetaData;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 单例模式
 */
@Slf4j
public class DefaultLogSysModule implements  LogSysModule{

    public static String dbDir;
    public static String logsDir;

    private static RocksDB logDb;

    public final static byte[] LAST_INDEX_KEY = "last_index".getBytes();

    public final static byte[] META_DATA_KEY = "meta_data".getBytes();
    private ReentrantLock lock = new ReentrantLock();

    private DefaultLogSysModule(){
        //init
        Options options = new Options();
        options.setCreateIfMissing(true);

        File file = new File(logsDir);
        boolean success = false;
        if (!file.exists()) {
            success = file.mkdirs();
        }
        if (success) {
            log.warn("make a new dir : " + logsDir);
        }
        try {
            logDb = RocksDB.open(options, logsDir);
        } catch (RocksDBException e) {
            log.error(e.getMessage());
        }
    }

    private static class SingletonHolder {
        private static final DefaultLogSysModule defaultLogSysModule = new DefaultLogSysModule();
    }

    public static DefaultLogSysModule getInstance(){
        return SingletonHolder.defaultLogSysModule;
    }
    @Override
    public void write(LogEntry logEntry) {
        boolean success = false;
        try {
            lock.tryLock(3000, MILLISECONDS);
            logEntry.setIndex(getLastIndex() + 1);
            logDb.put(String.valueOf(logEntry.getIndex()).getBytes(), JSON.toJSONBytes(logEntry));
            success = true;
            log.info("DefaultLog write rocksDB success, logEntry info : [{}]", logEntry);
        } catch (RocksDBException | InterruptedException e) {
            log.warn(e.getMessage());
        } finally {
            if (success) {
                updateLastIndex(logEntry.getIndex());
            }
            lock.unlock();
        }
    }

    @Override
    public LogEntry read(Long index) {
        try {
            byte[] result = logDb.get(convert(index));
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            log.warn(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public void removeOnStartIndex(Long startIndex) {
        boolean success = false;
        int count = 0;

        boolean lockres = false;
        try {
            lockres = lock.tryLock(3000, MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(lockres) {
                try {
                for (long i = startIndex; i <= getLastIndex(); i++) {
                    logDb.delete(String.valueOf(i).getBytes());
                    ++count;
                }
                success = true;
                log.warn("rocksDB removeOnStartIndex success, count={} startIndex={}, lastIndex={}", count, startIndex, getLastIndex());
            } catch ( RocksDBException e) {
                log.warn(e.getMessage());
            } finally {
                if (success) {
                    updateLastIndex(getLastIndex() - count);
                }
                lock.unlock();
            }
            }

    }

    @Override
    public LogEntry getLast() {
        try {
            byte[] result = logDb.get(convert(getLastIndex()));
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long getLastIndex() {
        byte[] lastIndex = "-1".getBytes();
        try {
            lastIndex = logDb.get(LAST_INDEX_KEY);
            if (lastIndex == null) {
                lastIndex = "-1".getBytes();
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Long.valueOf(new String(lastIndex));
    }

    @Override
    public MetaData getMetaData() {
        try {
            MetaData metaData = JSON.parseObject(logDb.get(META_DATA_KEY), MetaData.class);
            return metaData;
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    @SneakyThrows
    @Override
    public void setMetaData(long currentTerm, String votedFor, long lastAppliedIndex, long commitIndex) {
        MetaData metaData = new MetaData();
        metaData.setCommitIndex(commitIndex);
        metaData.setCurrentTerm(currentTerm);
        metaData.setLastAppliedIndex(lastAppliedIndex);
        metaData.setVotedFor(votedFor);
        logDb.put(META_DATA_KEY, JSON.toJSONString(metaData).getBytes(StandardCharsets.UTF_8));
    }

    //in lock
    private void updateLastIndex(Long index) {
        try {
            // overWrite
            logDb.put(LAST_INDEX_KEY, index.toString().getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
    private byte[] convert(Long key) {
        return key.toString().getBytes();
    }
}
