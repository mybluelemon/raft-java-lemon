package Model;

import lombok.Data;

@Data
public class RVoteParam {

    /**
     * vote请求
     * term -> 当前自己的term
     * candidateId -> 为谁投票，一般发起都设为自己
     * lastLogIndex -> 存储的日志最后的Index ,持久化，但可能没commit
     * lastLogTerm -> 日志里的最大Term
     *
     */

    long term;

    String candidateId;

    long lastLogIndex;

    long lastLogTerm;

}
