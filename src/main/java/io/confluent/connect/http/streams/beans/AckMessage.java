package io.confluent.connect.http.streams.beans;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AckMessage {
    private boolean isSucess;
    private boolean isError;
    private boolean ackStatus;
    private String responseMessage;
    private String sourceMessage;
}
