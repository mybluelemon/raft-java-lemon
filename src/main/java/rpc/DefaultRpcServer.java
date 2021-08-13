package rpc;

import Model.AppendEntryParam;
import Model.ClientKVReq;
import Model.RVoteParam;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AbstractUserProcessor;
import common.DefaultNode;
import lombok.Data;

@Data
public class DefaultRpcServer {

    private volatile boolean flag;

    private DefaultNode node;

    private com.alipay.remoting.rpc.RpcServer rpcServer;

    public DefaultRpcServer(int port, DefaultNode node) {

        if (flag) {
            return;
        }
        synchronized (this) {
            if (flag) {
                return;
            }

            rpcServer = new com.alipay.remoting.rpc.RpcServer(port, false, false);

            rpcServer.registerUserProcessor(new RaftUserProcessor<Request>() {

                @Override
                public Object handleRequest(BizContext bizCtx, Request request) throws Exception {
                    return handlerRequest(request);
                }
            });

            this.node = node;
            flag = true;
        }

    }


    public void start() {
        rpcServer.start();
    }


    public void stop() {
        rpcServer.stop();
    }


    public Response handlerRequest(Request request) {
        if (request.getCmd() == Request.R_VOTE) {
            return new Response(node.handlerRequestVote((RVoteParam) request.getObj()));
        } else if (request.getCmd() == Request.A_ENTRIES) {
            return new Response(node.handlerAppendEntries((AppendEntryParam) request.getObj()));
        } else if (request.getCmd() == Request.CLIENT_REQ) {
            return new Response(node.handlerClientRequest((ClientKVReq) request.getObj()));
        }
        return null;
    }

}
