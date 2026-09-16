package com.github.alexeyklimov.featurestore.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.junit.jupiter.api.Test;

class LegacyReadServiceTest {
    @Test
    void abortsStreamingWhenInflightRequestAndResponseExceedQuota() throws Exception {
        var keyType = FeatureCatalog.KeyType.of(
                "user_id",
                1,
                "user_features",
                new FeatureCatalog.FeatureDefinition("feature1", 101, FeatureCatalog.ValueEncoding.INT32));
        var requestBody = """
                {
                  "keys": [{"user_id": "userA"}],
                  "features": ["feature1"]
                }
                """;

        long requestBytes;
        try (var allocator = new RootAllocator()) {
            var codec = new LegacyJsonArrowCodec(new FeatureCatalog(List.of(keyType), List.of()));
            try (var request = codec.parse(
                    new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8)),
                    allocator)) {
                requestBytes = request.arrowBytes();
            }
        }

        var responseBatchHolder = newResponseBatch();
        var responseBytes = ArrowMessages.vectorRootBytes(responseBatchHolder.batch);
        var quota = requestBytes + responseBytes - 1;
        var catalog = new FeatureCatalog(
                List.of(keyType),
                List.of(new FeatureCatalog.TenantPolicy(
                        "tenant-a",
                        Set.of("user_id"),
                        new FeatureCatalog.RequestQuota(10, 10, quota))));

        var calls = new AtomicInteger();
        try (var allocator = new RootAllocator();
             responseBatchHolder) {
            var service = new LegacyReadService(
                    allocator,
                    new LegacyJsonArrowCodec(catalog),
                    new TenantAccessController(catalog),
                    (request, ignoredAllocator, consumer) -> {
                        calls.incrementAndGet();
                        consumer.accept(request, responseBatchHolder.batch);
                        calls.incrementAndGet();
                        consumer.accept(request, responseBatchHolder.batch);
                    });

            try (var prepared = service.prepare(
                    "tenant-a",
                    new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8)))) {
                var output = new ByteArrayOutputStream();
                assertThatThrownBy(() -> prepared.stream(output))
                        .isInstanceOf(ReadRequestException.class)
                        .hasMessageContaining("Tenant quota exceeded");
                assertThat(calls).hasValue(1);
            }
        }
    }

    private static ResponseBatchHolder newResponseBatch() {
        var allocator = new RootAllocator();
        var batch = ArrowMessages.newResultRoot(allocator);
        batch.allocateNew();
        ((BigIntVector) batch.getVector("request_ordinal")).setSafe(0, 0L);
        ((VarBinaryVector) batch.getVector("entity")).setSafe(0, "userA".getBytes(StandardCharsets.UTF_8));
        ((IntVector) batch.getVector("feature_id")).setSafe(0, 101);
        ((VarBinaryVector) batch.getVector("value")).setSafe(0, intBytes(1));
        batch.setRowCount(1);
        return new ResponseBatchHolder(allocator, batch);
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(value)
                .array();
    }

    private static final class ResponseBatchHolder implements AutoCloseable {
        private final RootAllocator allocator;
        private final org.apache.arrow.vector.VectorSchemaRoot batch;

        private ResponseBatchHolder(RootAllocator allocator, org.apache.arrow.vector.VectorSchemaRoot batch) {
            this.allocator = allocator;
            this.batch = batch;
        }

        @Override
        public void close() {
            batch.close();
            allocator.close();
        }
    }
}
