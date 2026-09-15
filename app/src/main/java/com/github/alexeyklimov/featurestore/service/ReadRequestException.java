package com.github.alexeyklimov.featurestore.service;

public final class ReadRequestException extends RuntimeException {
    private final int statusCode;

    /** Создает ошибку запроса с HTTP-статусом. */
    public ReadRequestException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    /** Возвращает HTTP-статус ошибки. */
    public int statusCode() {
        return statusCode;
    }
}
