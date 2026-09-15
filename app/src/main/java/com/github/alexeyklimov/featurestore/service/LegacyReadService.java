package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.cassandra.CassandraSliceReadPipe;
import java.io.InputStream;
import org.apache.arrow.memory.RootAllocator;

public final class LegacyReadService {
    private final RootAllocator allocator;
    private final LegacyJsonArrowCodec codec;
    private final TenantAccessController accessController;
    private final CassandraSliceReadPipe sliceReadPipe;

    /** Создает сервис чтения со всеми зависимостями. */
    public LegacyReadService(
            RootAllocator allocator,
            LegacyJsonArrowCodec codec,
            TenantAccessController accessController,
            CassandraSliceReadPipe sliceReadPipe
    ) {
        this.allocator = allocator;
        this.codec = codec;
        this.accessController = accessController;
        this.sliceReadPipe = sliceReadPipe;
    }

    /** Готовит авторизованный запрос к чтению. */
    public PreparedRead prepare(String tenantId, InputStream inputStream) throws Exception {
        var request = codec.parse(inputStream, allocator);
        try {
            return new PreparedRead(accessController.authorize(tenantId, request));
        } catch (Exception exception) {
            request.close();
            throw exception;
        }
    }

    public final class PreparedRead implements AutoCloseable {
        private final ArrowMessages.ArrowTenantRequest request;

        /** Сохраняет подготовленный и проверенный запрос. */
        private PreparedRead(ArrowMessages.ArrowTenantRequest request) {
            this.request = request;
        }

        /** Выполняет чтение и пишет ответ в поток. */
        public void stream(java.io.OutputStream outputStream) throws Exception {
            try (var writer = codec.newResponseWriter(outputStream)) {
                for (var sliceRequest : request.sliceRequests()) {
                    sliceReadPipe.stream(
                            sliceRequest,
                            allocator,
                            (currentRequest, batch) -> writer.consume(currentRequest, batch));
                }
            }
        }

        /** Освобождает ресурсы подготовленного чтения. */
        @Override
        public void close() {
            request.close();
        }
    }
}
