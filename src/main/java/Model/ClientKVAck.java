package Model;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Data
public class ClientKVAck implements Serializable {
int code ;
String leaderId;
    Object result;




}
