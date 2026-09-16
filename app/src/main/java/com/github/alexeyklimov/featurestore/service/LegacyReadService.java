package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.cassandra.CassandraSliceReadPipe;
import java.io.InputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public final class LegacyReadService {
    private final RootAllocator allocator;
    private final LegacyJsonArrowCodec codec;
    private final TenantAccessController accessController;
    private final SliceReadStreamer sliceReadStreamer;

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
        this.sliceReadStreamer = sliceReadPipe::stream;
    }

    LegacyReadService(
            RootAllocator allocator,
            LegacyJsonArrowCodec codec,
            TenantAccessController accessController,
            SliceReadStreamer sliceReadStreamer
    ) {
        this.allocator = allocator;
        this.codec = codec;
        this.accessController = accessController;
        this.sliceReadStreamer = sliceReadStreamer;
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
        private final TenantAccessController.AuthorizedTenantRequest authorizedRequest;

        /** Сохраняет подготовленный и проверенный запрос. */
        private PreparedRead(TenantAccessController.AuthorizedTenantRequest authorizedRequest) {
            this.authorizedRequest = authorizedRequest;
        }

        /** Выполняет чтение и пишет ответ в поток. */
        public void stream(java.io.OutputStream outputStream) throws Exception {
            try (var writer = codec.newResponseWriter(outputStream)) {
                for (var sliceRequest : authorizedRequest.request().sliceRequests()) {
                    sliceReadStreamer.stream(
                            sliceRequest,
                            allocator,
                            (currentRequest, batch) -> consumeBatch(writer, currentRequest, batch));
                }
            }
        }

        private void consumeBatch(
                LegacyJsonArrowCodec.JsonArrowResponseWriter writer,
                ArrowMessages.SliceReadRequest currentRequest,
                VectorSchemaRoot batch
        ) throws Exception {
            var batchBytes = ArrowMessages.vectorRootBytes(batch);
            accessController.reserveResponseInflightBytes(authorizedRequest, batchBytes);
            try {
                writer.consume(currentRequest, batch);
            } finally {
                accessController.releaseResponseInflightBytes(authorizedRequest, batchBytes);
            }
        }

        /** Освобождает ресурсы подготовленного чтения. */
        @Override
        public void close() {
            authorizedRequest.close();
        }
    }

    @FunctionalInterface
    interface SliceReadStreamer {
        void stream(
                ArrowMessages.SliceReadRequest request,
                BufferAllocator allocator,
                ArrowMessages.ResultBatchConsumer consumer
        ) throws Exception;
    }
}
