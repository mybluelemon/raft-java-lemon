package Model;

import lombok.Data;

import java.io.Serializable;

@Data
public class Command implements Serializable {
    String key;

    String value;
}
