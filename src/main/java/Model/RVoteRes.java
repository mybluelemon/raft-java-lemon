package Model;


import lombok.Data;

@Data
public class RVoteRes {

    //vote res
    /**
     *  RequestTerm < currentTerm ->  currentTerm , false
     *
     * votedFor -》 已经vote别人 -》 （currentTerm , false）
     *
     * votedFor == null || votedFor == candidateId and log term , and log index
     *   -> 更新本地的term , voted for 变成 follower return (RequestTerm, true)
     */
   // term
    long term;

    //yes or no
    boolean voteGranted;
}
