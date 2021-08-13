package common;

import Model.*;
import ch.qos.logback.core.joran.util.beans.BeanUtil;
import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.rpc.RpcServer;
import jdk.nashorn.internal.runtime.regexp.joni.constants.NodeStatus;
import log.DefaultLogSysModule;
import log.LogSysModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import rpc.DefaultRpcClient;
import rpc.Request;
import rpc.Response;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * node 的一些状态
 * 持久化的
 * currentTerm -> 当前term，充当逻辑时钟 ，不能从log里面读，
 * votedFor -> 投票给谁，为保证每个term只能投一次，持久化
 * log[] 持久化日志
 *
 * volatile
 * commitIndex -》
 * lastAppliedIndex -> stateMachine 也持久化了，这个也可以持久化，要不然得重新运行一遍所有日志
 *
 * server独有的
 * nextIndex[] 每个节点的下条要发送的appendEntryIndex，初始化leader的lastLogIndex
 * matchIndex[] 每个节点与leader对上了的index，默认0
 */
@Slf4j
public class DefaultNode {
    private ConcurrentMap<String, Peer> peerMap = new ConcurrentHashMap<>();

    private Set<Peer> peerSet = new HashSet<>();

    private String localServer ;
    //状态机
    private StateMachine stateMachine;

    //日志存储系统
    private LogSysModule logSysModule;

    //默认是
    private int state = NodeState.FOLLOWER;
    //持久化
    private long currentTerm ;

    //持久化
    private String votedFor;

    private String leaderId;

    //不需要持久化，可以被恢复
    private long commitIndex;

    //在本实现中也持久化，节省时间
    private long  lastAppliedIndex;

    //所有关于currentTerm status votedFor 的读取，更新，都要保持一致性
    private Lock lock = new ReentrantLock();

    //需要并发控制 concurrentHashMap
    /** 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一） */
    Map<Peer, Long> nextIndexes = new ConcurrentHashMap<>();

    //需要并发控制 concurrentHashMap
    /** 对于每一个服务器，已经复制给他的日志的最高索引值 */
    Map<Peer, Long> matchIndexes = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    ThreadPoolExecutor threadPool = new ThreadPoolExecutor (10,20,0, TimeUnit.MILLISECONDS, new LinkedBlockingDeque(1000));
    //节点状态控制
    public volatile boolean started;
    public NodeConfig config;

    //节点需要接受别人的请求
    public static RpcServer RPC_SERVER;

    //节点需要主动发送请求
    public DefaultRpcClient rpcClient = new DefaultRpcClient();

    private DefaultNode() {
        if (started) {
            return;
        }
        RPC_SERVER.start();
        stateMachine = DefaultStateMachine.getInstance();
        logSysModule = DefaultLogSysModule.getInstance();
        //currentTerm votedFor lastAppliedIndex
        currentTerm = logSysModule.getMetaData().getCurrentTerm();
        votedFor = logSysModule.getMetaData().getVotedFor();
        //这两个不要求准确
        lastAppliedIndex = logSysModule.getMetaData().getLastAppliedIndex();
        commitIndex = logSysModule.getMetaData().getCommitIndex();
        //nextIndexes 可以先不初始化


        //heatbeat task

        //election task

        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        heartbeatScheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new HeartBeatTask(), 0, 10, TimeUnit.SECONDS);
        electionScheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new ElectionTask(), 0, 20, TimeUnit.SECONDS);

    }

    private static class SingletonHolder {
        private  static  final DefaultNode node = new DefaultNode();
    }
    public static DefaultNode getInstance() {
        return SingletonHolder.node;
    }


     class HeartBeatTask  implements Runnable {

        @Override
        public void run() {
            //heart beat 给所有的peer发心跳， appendentries ,发nextIndex ，自己的lastLog,
            AppendEntryParam param = new AppendEntryParam();
            param.setLeaderCommit(commitIndex);
            param.setTerm(currentTerm);
            param.setLeaderId(leaderId);
            AtomicInteger count =  new AtomicInteger(1);
for(Peer peer: peerSet){
    //
    threadPool.execute(new Runnable() {
        @Override
        public void run() {
            appendEntries(peer, param,count);
        }
    });


}

        }
    }


    public RVoteRes handlerRequestVote(RVoteParam param) {
        log.warn("handlerRequestVote will be invoke, param info : {}", param);
        RVoteRes res = new RVoteRes();
        res.setVoteGranted(false);
       //投票请求应答
        // term , lastlogIndex , lastlogTerm, 合格votedfor 持久化， 就变成follower
        if(param.getTerm() < currentTerm){
            res.setTerm(currentTerm);
            return res;
        }
        lock.lock();
        try {
            if (param.getTerm() > currentTerm) {
                stepDown(param.getTerm());
            }
            res.setTerm(currentTerm);
            LogEntry last = logSysModule.getLast();
            if (param.getLastLogTerm() < last.getTerm() ||
                    (param.getTerm() == last.getTerm() && param.getLastLogIndex() < last.getIndex())) {

                return res;
            }
            //votedfor
            stepDown(param.getTerm());
            votedFor = param.getCandidateId();
            logSysModule.setMetaData(currentTerm, votedFor, lastAppliedIndex, commitIndex);
        }finally {
            lock.unlock();
        }
        res.setVoteGranted(true);
        return  res;
    }


    public AppendEntryRes handlerAppendEntries(AppendEntryParam param) {
        if (param.getEntries() != null) {
            log.warn("node receive node {} append entry, entry content = {}", param.getLeaderId(), param.getEntries());
        }
        AppendEntryRes result = new AppendEntryRes();
        result.setSuccess(false);

        lock.lock();
        try {
            result.setTerm(currentTerm);
            // 不够格
            if (param.getTerm() < currentTerm) {
                return result;
            }
            if (param.getTerm() >= currentTerm) {


                state = NodeState.FOLLOWER;
            }
            currentTerm = (param.getTerm());

            //处理日志匹配
            long prevLogTerm = param.getPrevLogTerm();
            long prevLonIndex = param.getPrevLonIndex();

            if (prevLonIndex != 0) {
                //check conflict
                LogEntry localLog = logSysModule.read(prevLonIndex);
                if (localLog == null || localLog.getTerm() != prevLogTerm) {
                    return result;
                }
            }
            //appendentries
            //
            result.setSuccess(true);
            for (LogEntry entry : param.getEntries()) {
                long curTerm = entry.getTerm();
                long curIndex = entry.getIndex();
                LogEntry oldEntry = logSysModule.read(curIndex);
                if (oldEntry != null && oldEntry.getTerm() != curTerm) {
                    //conflict truncate later all entries
                    logSysModule.removeOnStartIndex(curIndex);
                }
                logSysModule.write(entry);
            }

            //是否要commit,apply

            long newCommitIndex = Math.min(param.getLeaderCommit(),
                    param.getPrevLonIndex() + param.getEntries().size());
            commitIndex = (newCommitIndex);
            logSysModule.setMetaData(currentTerm, votedFor, lastAppliedIndex, newCommitIndex);
            if(lastAppliedIndex < commitIndex){
                for(long i = lastAppliedIndex+1 ; i <= commitIndex; i++){
                    stateMachine.apply(logSysModule.read(i));
                }

            }
        }catch (Exception e){
        }finally {
            lock.unlock();
        }
        return result;
    }


    class ElectionTask implements  Runnable {

        @Override
        public void run() {
            if(state == NodeState.LEADER){
                return;
            }
            //elect
   // term + 1 . state ->  candidate , votedFor -> self , send the request

            lock.lock();
            try{
                currentTerm++;
                state = NodeState.CANDIDATE;
                votedFor = localServer;
                long savedTerm = currentTerm;

                AtomicInteger count = new AtomicInteger(1);
                for(Peer peer: peerSet){
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            RVoteParam param = new RVoteParam();
                            param.setTerm(currentTerm);
                            param.setCandidateId(votedFor);
                            param.setLastLogIndex(logSysModule.getLastIndex());
                            param.setLastLogTerm(logSysModule.getLast().getTerm());
                            Request<RVoteParam> request = new Request<>();
                            request.setCmd(Request.R_VOTE);
                            request.setObj(param);
                            request.setUrl(peer.getAddr());
                            Response<RVoteRes> res = (Response<RVoteRes>) rpcClient.send(request, 2000);
                            if(res ==  null){
                                return ;
                            }
                            if(res.getResult().getTerm() > savedTerm){
                                stepDown(res.getResult().getTerm());
                            }else {
                                //
                                if(savedTerm == res.getResult().getTerm()) {
                                    if (res.getResult().isVoteGranted()) {
                                        count.incrementAndGet();
                                    }
                                    if (2 * count.get() >= peerSet.size()) {
                                        if(savedTerm == currentTerm) {
                                            becomeLeader();
                                        }
                                    }
                                }
                            }
                        }
                    });



                }
            }finally {
                lock.unlock();
            }

        }
    }

    private void becomeLeader() {
        state = NodeState.LEADER;
        leaderId = localServer;
        // stop vote timer
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        // start heartbeat timer
        heartbeatScheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new HeartBeatTask(), 0, 10, TimeUnit.SECONDS);
    }
    public void stepDown(long newTerm) {
        if (currentTerm > newTerm) {
            log.error("can't be happened");
            return;
        }
        if (currentTerm < newTerm) {
            currentTerm = newTerm;
            leaderId = "";
            votedFor = "";
            logSysModule.setMetaData(currentTerm, votedFor, lastAppliedIndex, commitIndex);
        }
        state = NodeState.FOLLOWER;
        // stop heartbeat
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        resetElectionTimer();
    }

    private void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        electionScheduledFuture = scheduledExecutorService.scheduleAtFixedRate(new ElectionTask(), 0, 20, TimeUnit.SECONDS);
    }

    public void appendEntries(Peer peer, AppendEntryParam param , AtomicInteger count){
        if(state != NodeState.LEADER || currentTerm != param.getTerm()){
            return;
        }
AppendEntryParam selfParam = new AppendEntryParam();
        try {
            BeanUtils.copyProperties(selfParam, param);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        //
        Long nextIndex = nextIndexes.get(peer);
        long preLogIndex = nextIndex -1 ;
        long preLogTerm = preLogIndex == 0 ? 0 : logSysModule.read(preLogIndex).getTerm();
        long endIndex = logSysModule.getLastIndex();

        selfParam.setEntries(getEntries(nextIndex, endIndex));
selfParam.setPrevLonIndex(preLogIndex);
selfParam.setPrevLogTerm(preLogTerm);



        Request<AppendEntryParam> request = new Request<>(
                Request.A_ENTRIES,
                selfParam,
                peer.getAddr());
        Response<AppendEntryRes> res = (Response<AppendEntryRes>)rpcClient.send(request);
        if(res.getResult().getTerm() > currentTerm){
            stepDown(res.getResult().getTerm());
            return;
        }
        lock.lock();
        try {
            if (res.getResult().isSuccess()) {
                //成功，更新nextIndex,matchIndex
                matchIndexes.put(peer, preLogIndex + selfParam.getEntries().size());
                nextIndexes.put(peer, matchIndexes.get(peer) + 1);
                //计算commitIndex;
                Collection<Long> values = matchIndexes.values();
                ArrayList<Long> indexes = new ArrayList<>(values);
                indexes.sort(new Comparator<Long>() {
                    @Override
                    public int compare(Long o1, Long o2) {
                        return (int) (o1 - o2);
                    }
                });
                Long index = indexes.get(indexes.size() / 2);
                LogEntry midEntry = logSysModule.read(index);
                if (midEntry.getTerm() != currentTerm) {
                    return;
                }
                long newCommitIndex = midEntry.getIndex();
                if (newCommitIndex > commitIndex) {
                    commitIndex = newCommitIndex;
                }
                logSysModule.setMetaData(currentTerm, votedFor, commitIndex, lastAppliedIndex);
                //apply
                if (lastAppliedIndex < commitIndex) {
                    for (long i = lastAppliedIndex + 1; i <= commitIndex; i++) {
                        stateMachine.apply(logSysModule.read(i));
                    }

                }
            }
        }catch (Exception e){

        }finally {
            lock.unlock();
        }

    }

    private List<LogEntry> getEntries(long startIndex , long endIndex){
        List<LogEntry> list = new ArrayList<>();
        for(long i = startIndex ; i <= endIndex ; i++){
            LogEntry entry = logSysModule.read(i);
            if(entry == null){
                return list;
            }
            list.add(entry);
        }
        return list;
    }
    public  ClientKVAck handlerClientRequest(ClientKVReq request){

        //client kv
        if(state != NodeState.LEADER){
            ClientKVAck ack = new ClientKVAck();
            ack.setLeaderId(leaderId);
            ack.setCode(1);
            return ack;

        }
        lock.lock();
        if(request.getType() == ClientKVReq.GET){
            ClientKVAck ack = new ClientKVAck();
            ack.setCode(0);
            //get
            LogEntry entry = stateMachine.get(request.getKey());
            if(entry != null){
                ack.setResult(entry.getCommand().getValue());

            }
            return ack;
        }else{
            //write log

        }
        return null;
    }
}
