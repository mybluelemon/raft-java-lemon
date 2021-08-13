package rpc;


import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcResponseFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 * @author 莫那·鲁道
 */
public class DefaultRpcClient  {

    public static Logger logger = LoggerFactory.getLogger(DefaultRpcClient.class.getName());

    private final static com.alipay.remoting.rpc.RpcClient CLIENT = new com.alipay.remoting.rpc.RpcClient();
    static {
        CLIENT.init();
    }



    public Response send(Request request) {
        return send(request, 200000);
    }


    public Response send(Request request, int timeout) {
        Response result = null;
        try {
            result = (Response) CLIENT.invokeSync(request.getUrl(), request, timeout);
        } catch (RemotingException e) {
            e.printStackTrace();
            logger.info("rpc RaftRemotingException ");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return (result);
    }

    public RpcResponseFuture sendAsync(Request request){
        try {
            RpcResponseFuture rpcResponseFuture = CLIENT.invokeWithFuture(request.getUrl(), request, 20000);
            return rpcResponseFuture;

        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return  null;
    }
    public void sendCallBack(Request request, InvokeCallback callback){
        try {
             CLIENT.invokeWithCallback(request.getUrl(), request,callback, 20000);


        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
