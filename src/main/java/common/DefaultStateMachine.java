package common;

import Model.Command;
import Model.LogEntry;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;

/**
 * 是一个kv 系统，所以说，我们需要基本实现
 * 用rockDB，持久化，
 */

/**
 * 单例
 */
@Slf4j
public class DefaultStateMachine implements  StateMachine{
    public static String dbDir;
    public static String stateMachineDir;

    public static RocksDB machineDb;
    static {
        if (dbDir == null) {
            dbDir = "./rocksDB-raft/" + System.getProperty("serverPort");
        }
        if (stateMachineDir == null) {
            stateMachineDir =dbDir + "/stateMachine";
        }
        RocksDB.loadLibrary();
    }

    private DefaultStateMachine(){
        //something init
        try {
            File file = new File(stateMachineDir);
            boolean success = false;
            if (!file.exists()) {
                success = file.mkdirs();
            }
            if (success) {
                log.warn("make a new dir : " + stateMachineDir);
            }
            Options options = new Options();
            options.setCreateIfMissing(true);
            machineDb = RocksDB.open(options, stateMachineDir);

        } catch (RocksDBException e) {
            log.info("stateMechine init error",e);
        }
    }

    private static class SingletonHolder{
        private static  final  DefaultStateMachine defaultStateMachine = new DefaultStateMachine();
    }

    public static DefaultStateMachine getInstance(){
        return SingletonHolder.defaultStateMachine;
    }
    @Override
    public LogEntry get(String key) {
        try {
            byte[] result = machineDb.get(key.getBytes());
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            log.info(e.getMessage());
        }
        return null;
    }

    @Override
    public String getString(String key) {
        try {
            byte[] bytes = machineDb.get(key.getBytes());
            if (bytes != null) {
                return new String(bytes);
            }
        } catch (RocksDBException e) {
            log.info(e.getMessage());
        }
        return "";
    }

    @Override
    public void setString(String key, String value) {
        try {
            machineDb.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            log.info(e.getMessage());
        }
    }

    @Override
    public void delString(String... key) {
        try {
            for (String s : key) {
                machineDb.delete(s.getBytes());
            }
        } catch (RocksDBException e) {
            log.info(e.getMessage());
        }
    }

    @Override
    public synchronized void apply(LogEntry logEntry) {

        try {
            Command command = logEntry.getCommand();

            if (command == null) {
                throw new IllegalArgumentException("command can not be null, logEntry : " + logEntry.toString());
            }
            String key = command.getKey();
            machineDb.put(key.getBytes(), JSON.toJSONBytes(logEntry));
        } catch (RocksDBException e) {
            log.info(e.getMessage());
        }
    }
}
