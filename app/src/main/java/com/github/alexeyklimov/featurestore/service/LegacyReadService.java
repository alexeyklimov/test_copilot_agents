package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.cassandra.CassandraSliceReadPipe;
import java.io.InputStream;
import org.apache.arrow.memory.RootAllocator;

public final class LegacyReadService {
    private final RootAllocator allocator;
    private final LegacyJsonArrowCodec codec;
    private final TenantAccessController accessController;
    private final CassandraSliceReadPipe sliceReadPipe;

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

    public PreparedRead prepare(String tenantId, InputStream inputStream) throws Exception {
        var request = accessController.authorize(tenantId, codec.parse(inputStream, allocator));
        return new PreparedRead(request);
    }

    public final class PreparedRead implements AutoCloseable {
        private final ArrowMessages.ArrowTenantRequest request;

        private PreparedRead(ArrowMessages.ArrowTenantRequest request) {
            this.request = request;
        }

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

        @Override
        public void close() {
            request.close();
        }
    }
}
