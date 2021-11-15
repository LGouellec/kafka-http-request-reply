package io.confluent.connect.http.streams.beans;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IntermediateMessage {
    private boolean isSucess;
    private boolean isError;
    private String message;

    public IntermediateMessage(String message, boolean isSucess){
        setMessage(message);
        setSucess(isSucess);
        setError(!isSucess);
    }
}
