package com.github.alexeyklimov.featurestore.service;

public final class ReadRequestException extends RuntimeException {
    private final int statusCode;

    public ReadRequestException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }
}
