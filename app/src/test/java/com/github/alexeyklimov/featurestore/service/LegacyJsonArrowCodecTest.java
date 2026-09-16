package com.github.alexeyklimov.featurestore.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.junit.jupiter.api.Test;

class LegacyJsonArrowCodecTest {
    @Test
    void streamsResponseRowsAcrossBatchBoundaries() throws Exception {
        var keyType = FeatureCatalog.KeyType.of(
                "user_id",
                1,
                "user_features",
                new FeatureCatalog.FeatureDefinition("feature1", 101, FeatureCatalog.ValueEncoding.INT32),
                new FeatureCatalog.FeatureDefinition("feature2", 102, FeatureCatalog.ValueEncoding.INT32),
                new FeatureCatalog.FeatureDefinition("feature3", 103, FeatureCatalog.ValueEncoding.UTF8));
        var codec = new LegacyJsonArrowCodec(new FeatureCatalog(List.of(keyType), List.of()));

        try (var allocator = new RootAllocator();
             var requestRoot = ArrowMessages.newRequestRoot(allocator);
             var firstBatch = ArrowMessages.newResultRoot(allocator);
             var secondBatch = ArrowMessages.newResultRoot(allocator);
             var request = new ArrowMessages.SliceReadRequest(keyType, requestRoot, new int[]{101, 102, 103})) {
            var output = new ByteArrayOutputStream();
            requestRoot.allocateNew();
            firstBatch.allocateNew();
            secondBatch.allocateNew();
            writeRow(firstBatch, 0, 0, "userA", 101, intBytes(1));
            writeRow(secondBatch, 0, 0, "userA", 102, intBytes(2));
            writeRow(secondBatch, 1, 1, "userB", 103, "ready".getBytes(StandardCharsets.UTF_8));

            try (var writer = codec.newResponseWriter(output)) {
                writer.consume(request, firstBatch);
                writer.consume(request, secondBatch);
            }

            assertThat(output.toString(StandardCharsets.UTF_8))
                    .isEqualTo(
                            """
                            [{"key":"user_id","key_value":"userA","features":{"feature1":1,"feature2":2}},{"key":"user_id","key_value":"userB","features":{"feature3":"ready"}}]"""
                                    .trim());
        }
    }

    private static void writeRow(
            org.apache.arrow.vector.VectorSchemaRoot batch,
            int row,
            long ordinal,
            String entity,
            int featureId,
            byte[] value
    ) {
        ((BigIntVector) batch.getVector("request_ordinal")).setSafe(row, ordinal);
        ((VarBinaryVector) batch.getVector("entity")).setSafe(row, entity.getBytes(StandardCharsets.UTF_8));
        ((IntVector) batch.getVector("feature_id")).setSafe(row, featureId);
        ((VarBinaryVector) batch.getVector("value")).setSafe(row, value);
        batch.setRowCount(Math.max(batch.getRowCount(), row + 1));
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(value)
                .array();
    }
}
