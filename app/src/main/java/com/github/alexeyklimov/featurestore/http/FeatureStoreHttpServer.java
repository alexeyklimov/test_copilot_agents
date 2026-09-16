package com.github.alexeyklimov.featurestore.http;

import com.github.alexeyklimov.featurestore.service.LegacyReadService;
import com.github.alexeyklimov.featurestore.service.ReadRequestException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

public final class FeatureStoreHttpServer implements AutoCloseable {
    private final HttpServer server;

    /** Оборачивает созданный экземпляр HttpServer. */
    private FeatureStoreHttpServer(HttpServer server) {
        this.server = server;
    }

    /** Создает HTTP-сервер с обработчиком чтения. */
    public static FeatureStoreHttpServer start(int port, LegacyReadService readService) throws IOException {
        var server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/read", new LegacyReadHandler(readService));
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        return new FeatureStoreHttpServer(server);
    }

    /** Возвращает порт запущенного сервера. */
    public int port() {
        return server.getAddress().getPort();
    }

    /** Запускает HTTP-сервер. */
    public void start() {
        server.start();
    }

    /** Останавливает HTTP-сервер. */
    @Override
    public void close() {
        server.stop(0);
    }

    private record LegacyReadHandler(LegacyReadService readService) implements HttpHandler {
        /** Обрабатывает входящий запрос legacy read API. */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                writeError(exchange, 405, "Only POST is supported");
                return;
            }

            var tenantId = exchange.getRequestHeaders().getFirst("X-Tenant-Id");
            if (tenantId == null || tenantId.isBlank()) {
                writeError(exchange, 400, "X-Tenant-Id header is required");
                return;
            }

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            try (var requestBody = exchange.getRequestBody()) {
                var preparedRead = readService.prepare(tenantId, requestBody);
                try (preparedRead) {
                    var responseFile = Path.of("/tmp").resolve("legacy-read-" + java.util.UUID.randomUUID() + ".json");
                    try {
                        try (var responseStream = Files.newOutputStream(responseFile)) {
                            preparedRead.stream(responseStream);
                        }
                        exchange.sendResponseHeaders(200, Files.size(responseFile));
                        try (var responseStream = Files.newInputStream(responseFile)) {
                            responseStream.transferTo(exchange.getResponseBody());
                        }
                    } finally {
                        Files.deleteIfExists(responseFile);
                    }
                }
            } catch (ReadRequestException exception) {
                if (!exchange.getResponseHeaders().containsKey("X-Error")) {
                    writeError(exchange, exception.statusCode(), exception.getMessage());
                }
            } catch (Exception exception) {
                writeError(exchange, 500, "Internal server error");
            } finally {
                exchange.close();
            }
        }

        /** Отправляет JSON-ошибку клиенту. */
        private static void writeError(HttpExchange exchange, int statusCode, String message) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("X-Error", "true");
            var payload = "{\"error\":\"" + message.replace("\"", "\\\"") + "\"}";
            var bytes = payload.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }
    }
}
