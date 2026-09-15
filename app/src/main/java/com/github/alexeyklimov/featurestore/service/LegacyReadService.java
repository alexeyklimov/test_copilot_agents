package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.cassandra.CassandraSliceReadPipe;
import java.io.InputStream;
import java.io.OutputStream;
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

    public void handle(String tenantId, InputStream inputStream, OutputStream outputStream) throws Exception {
        try (var request = accessController.authorize(tenantId, codec.parse(inputStream, allocator));
             var writer = codec.newResponseWriter(outputStream)) {
            for (var sliceRequest : request.sliceRequests()) {
                sliceReadPipe.stream(
                        sliceRequest,
                        allocator,
                        (currentRequest, batch) -> writer.consume(currentRequest, batch));
            }
        }
    }
}
