package com.zuzu.sg.review.splitter.exception;

public class S3FileProcessingException extends RuntimeException {
    public S3FileProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}

