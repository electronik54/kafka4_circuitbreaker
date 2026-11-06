package com.electronik54.consumer.downstream;

public class DownstreamUnavailableException extends RuntimeException {
    public DownstreamUnavailableException(String msg){
         super(msg);
    }
}
