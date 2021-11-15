package io.confluent.connect.http.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Random;


@RestController
@Slf4j
public class MessageController {

    private Random rd;
    private static final int percentError = 30;
    private static final int percentMessageKo = 50;

    public MessageController(){
        rd = new Random();
    }

    @PutMapping(path = "/api/messages")
    public ResponseEntity putMessage(@RequestBody String message, @RequestHeader Map<String, String> headers) {
        if(nextMessageSucceeded()) {
            headers.forEach((key, value) -> {
                log.info(String.format("Header '%s' = %s", key, value));
            });
            String msg = String.format("Message updated %s", nextMessageOk() ? "OK" : "KO");
            log.info(String.format("Process message succedded (message: %s, return message : %s", message, msg));
            return ResponseEntity
                    .status(HttpStatus.OK)
                    .body(msg);
        }
        else{
            log.info(String.format("Process message failed (message: %s)", message));
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .build();
        }
    }

    @PostMapping(path = "/api/messages")
    public ResponseEntity createMessage(@RequestBody String message, @RequestHeader Map<String, String> headers) {
        if(nextMessageSucceeded()) {
            headers.forEach((key, value) -> {
                log.info(String.format("Header '%s' = %s", key, value));
            });
            String msg = String.format("Message created %s", nextMessageOk() ? "OK" : "KO");

            log.info(String.format("Process message succedded (message: %s, return message: %s)", message, msg));
            return ResponseEntity
                    .status(HttpStatus.CREATED)
                    .body(msg);
        }
        else{
            log.info(String.format("Process message failed (message: %s)", message));
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .build();
        }
    }

    @DeleteMapping(path = "/api/messages/{id}")
    public ResponseEntity deleteMessage(@PathVariable String id) {
        log.warn("DELETING MESSAGE WITH ID: {}", id);

        try {
            Long lId = Long.valueOf(id);
        } catch (Exception e) {
            log.error("Unable to convert {} into a Long", id);
        }

        return ResponseEntity
                .status(HttpStatus.OK)
                .build();
    }

    private boolean nextMessageSucceeded(){
        int value = rd.nextInt(100);
        return value < 100-percentError;
    }

    private boolean nextMessageOk(){
        int value = rd.nextInt(100);
        return value < 100-percentMessageKo;
    }
}
