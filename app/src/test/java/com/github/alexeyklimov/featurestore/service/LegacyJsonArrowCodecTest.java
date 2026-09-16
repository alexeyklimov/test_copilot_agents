package com.github.alexeyklimov.featurestore.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import java.io.ByteArrayInputStream;
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
    private static final FeatureCatalog.KeyType USER_KEY_TYPE = FeatureCatalog.KeyType.of(
            "user_id",
            1,
            "user_features",
            new FeatureCatalog.FeatureDefinition("feature1", 101, FeatureCatalog.ValueEncoding.INT32),
            new FeatureCatalog.FeatureDefinition("feature2", 102, FeatureCatalog.ValueEncoding.INT32),
            new FeatureCatalog.FeatureDefinition("feature3", 103, FeatureCatalog.ValueEncoding.UTF8));
    private static final FeatureCatalog.KeyType CAR_KEY_TYPE = FeatureCatalog.KeyType.of(
            "car_id",
            2,
            "car_features",
            new FeatureCatalog.FeatureDefinition("feature4", 201, FeatureCatalog.ValueEncoding.INT32),
            new FeatureCatalog.FeatureDefinition("feature5", 202, FeatureCatalog.ValueEncoding.UTF8));
    private static final FeatureCatalog CATALOG = new FeatureCatalog(List.of(USER_KEY_TYPE, CAR_KEY_TYPE), List.of());

    @Test
    void parsesGroupedSliceRequestsAndTracksArrowFootprint() throws Exception {
        try (var allocator = new RootAllocator()) {
            long baseline = allocator.getAllocatedMemory();
            var codec = new LegacyJsonArrowCodec(CATALOG);
            ArrowMessages.ArrowTenantRequest request = codec.parse(new ByteArrayInputStream("""
                    {
                      "keys": [
                        {"user_id": "userA"},
                        {"car_id": "carA"},
                        {"user_id": "userB"}
                      ],
                      "features": ["feature1", "feature4", "feature1", "feature3"]
                    }
                    """.getBytes(StandardCharsets.UTF_8)), allocator);

            try (request) {
                assertThat(allocator.getAllocatedMemory()).isGreaterThan(baseline);
                assertThat(request.keyCount()).isEqualTo(3);
                assertThat(request.featureReferenceCount()).isEqualTo(9);
                assertThat(request.sliceRequests()).hasSize(2);

                var userRequest = request.sliceRequests().get(0);
                assertThat(userRequest.keyType()).isEqualTo(USER_KEY_TYPE);
                assertThat(userRequest.rowCount()).isEqualTo(2);
                assertThat(userRequest.requestOrdinal(0)).isEqualTo(0);
                assertThat(userRequest.requestOrdinal(1)).isEqualTo(2);
                assertThat(new String(userRequest.entity(0), StandardCharsets.UTF_8)).isEqualTo("userA");
                assertThat(new String(userRequest.entity(1), StandardCharsets.UTF_8)).isEqualTo("userB");
                assertThat(userRequest.featureIds()).containsExactly(101, 103);

                var carRequest = request.sliceRequests().get(1);
                assertThat(carRequest.keyType()).isEqualTo(CAR_KEY_TYPE);
                assertThat(carRequest.rowCount()).isEqualTo(1);
                assertThat(carRequest.requestOrdinal(0)).isEqualTo(1);
                assertThat(new String(carRequest.entity(0), StandardCharsets.UTF_8)).isEqualTo("carA");
                assertThat(carRequest.featureIds()).containsExactly(201);

                assertThat(request.arrowBytes()).isEqualTo(actualArrowBytes(request));
            }

            assertThat(allocator.getAllocatedMemory()).isEqualTo(baseline);
        }
    }

    @Test
    void increasesArrowFootprintForLargerRequestsAndReleasesMemoryOnClose() throws Exception {
        var codec = new LegacyJsonArrowCodec(CATALOG);

        long smallArrowBytes;
        try (var allocator = new RootAllocator()) {
            long baseline = allocator.getAllocatedMemory();
            ArrowMessages.ArrowTenantRequest request = codec.parse(new ByteArrayInputStream(requestJson(1, 1).getBytes(StandardCharsets.UTF_8)), allocator);
            try (request) {
                smallArrowBytes = request.arrowBytes();
                assertThat(request.arrowBytes()).isEqualTo(actualArrowBytes(request));
                assertThat(allocator.getAllocatedMemory()).isGreaterThan(baseline);
            }
            assertThat(allocator.getAllocatedMemory()).isEqualTo(baseline);
        }

        try (var allocator = new RootAllocator()) {
            long baseline = allocator.getAllocatedMemory();
            ArrowMessages.ArrowTenantRequest request = codec.parse(new ByteArrayInputStream(requestJson(32, 3).getBytes(StandardCharsets.UTF_8)), allocator);
            try (request) {
                assertThat(request.keyCount()).isEqualTo(32);
                assertThat(request.featureReferenceCount()).isEqualTo(96);
                assertThat(request.arrowBytes()).isEqualTo(actualArrowBytes(request));
                assertThat(request.arrowBytes()).isGreaterThan(smallArrowBytes);
                assertThat(allocator.getAllocatedMemory()).isGreaterThan(baseline);
            }
            assertThat(allocator.getAllocatedMemory()).isEqualTo(baseline);
        }
    }

    @Test
    void rejectsMalformedLegacyJsonRequests() {
        assertRejected("[]", "Request payload must be a JSON object");
        assertRejected("{}", "Request must contain at least one key");
        assertRejected("{\"keys\":[],\"features\":[\"feature1\"]}", "Request must contain at least one key");
        assertRejected("{\"keys\":[{\"user_id\":\"userA\"}]}", "Request must contain at least one feature");
        assertRejected("{\"keys\":[{\"user_id\":\"userA\"}],\"features\":[]}", "Request must contain at least one feature");
        assertRejected("{\"keys\":{},\"features\":[\"feature1\"]}", "'keys' must be an array");
        assertRejected("{\"keys\":[[]],\"features\":[\"feature1\"]}", "Each key entry must be an object");
        assertRejected("{\"keys\":[{}],\"features\":[\"feature1\"]}", "Each key entry must contain exactly one field");
        assertRejected(
                "{\"keys\":[{\"user_id\":\"userA\",\"car_id\":\"carA\"}],\"features\":[\"feature1\"]}",
                "Each key entry must contain exactly one field");
        assertRejected("{\"keys\":[{\"user_id\":1}],\"features\":[\"feature1\"]}", "Key values must be strings");
        assertRejected("{\"keys\":[{\"user_id\":\"userA\"}],\"features\":[1]}", "Feature names must be strings");
        assertRejected("{\"keys\":[{\"account_id\":\"userA\"}],\"features\":[\"feature1\"]}", "Unknown key type: account_id");
        assertRejected("{\"keys\":[{\"user_id\":\"userA\"}],\"features\":[\"feature404\"]}", "Unknown feature: feature404");
    }

    @Test
    void rejectsAmbiguousFeatureNamesAcrossKeyTypes() {
        var duplicateCatalog = new FeatureCatalog(List.of(
                FeatureCatalog.KeyType.of(
                        "user_id",
                        1,
                        "user_features",
                        new FeatureCatalog.FeatureDefinition("shared_feature", 101, FeatureCatalog.ValueEncoding.INT32)),
                FeatureCatalog.KeyType.of(
                        "car_id",
                        2,
                        "car_features",
                        new FeatureCatalog.FeatureDefinition("shared_feature", 201, FeatureCatalog.ValueEncoding.INT32))),
                List.of());

        assertRejected(
                new LegacyJsonArrowCodec(duplicateCatalog),
                "{\"keys\":[{\"user_id\":\"userA\"}],\"features\":[\"shared_feature\"]}",
                "Feature is bound to multiple key types: shared_feature");
    }

    @Test
    void streamsResponseRowsAcrossBatchBoundaries() throws Exception {
        var codec = new LegacyJsonArrowCodec(CATALOG);

        try (var allocator = new RootAllocator();
             var requestRoot = ArrowMessages.newRequestRoot(allocator);
             var firstBatch = ArrowMessages.newResultRoot(allocator);
             var secondBatch = ArrowMessages.newResultRoot(allocator);
             var request = new ArrowMessages.SliceReadRequest(USER_KEY_TYPE, requestRoot, new int[]{101, 102, 103})) {
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

    @Test
    void streamsMixedEncodingsAcrossEntitiesAndKeyTypes() throws Exception {
        var codec = new LegacyJsonArrowCodec(CATALOG);

        try (var allocator = new RootAllocator();
             var userRequestRoot = ArrowMessages.newRequestRoot(allocator);
             var carRequestRoot = ArrowMessages.newRequestRoot(allocator);
             var userBatch = ArrowMessages.newResultRoot(allocator);
             var carBatch = ArrowMessages.newResultRoot(allocator);
             var userRequest = new ArrowMessages.SliceReadRequest(USER_KEY_TYPE, userRequestRoot, new int[]{101, 103});
             var carRequest = new ArrowMessages.SliceReadRequest(CAR_KEY_TYPE, carRequestRoot, new int[]{201, 202})) {
            var output = new ByteArrayOutputStream();
            userRequestRoot.allocateNew();
            carRequestRoot.allocateNew();
            userBatch.allocateNew();
            carBatch.allocateNew();
            writeRow(userBatch, 0, 0, "userA", 101, intBytes(7));
            writeRow(userBatch, 1, 0, "userA", 103, "ready".getBytes(StandardCharsets.UTF_8));
            writeRow(userBatch, 2, 1, "userB", 103, "set".getBytes(StandardCharsets.UTF_8));
            writeRow(carBatch, 0, 2, "carA", 201, intBytes(11));
            writeRow(carBatch, 1, 2, "carA", 202, "electric".getBytes(StandardCharsets.UTF_8));

            try (var writer = codec.newResponseWriter(output)) {
                writer.consume(userRequest, userBatch);
                writer.consume(carRequest, carBatch);
            }

            assertThat(output.toString(StandardCharsets.UTF_8))
                    .isEqualTo(
                            """
                            [{"key":"user_id","key_value":"userA","features":{"feature1":7,"feature3":"ready"}},{"key":"user_id","key_value":"userB","features":{"feature3":"set"}},{"key":"car_id","key_value":"carA","features":{"feature4":11,"feature5":"electric"}}]"""
                                    .trim());
        }
    }

    @Test
    void writesEmptyArrayWhenNoBatchesAreConsumed() throws Exception {
        var codec = new LegacyJsonArrowCodec(CATALOG);
        var output = new ByteArrayOutputStream();

        try (var writer = codec.newResponseWriter(output)) {
            assertThat(output.toString(StandardCharsets.UTF_8)).isEmpty();
        }

        assertThat(output.toString(StandardCharsets.UTF_8)).isEqualTo("[]");
    }

    @Test
    void flushesFinalEntityOnlyWhenWriterCloses() throws Exception {
        var codec = new LegacyJsonArrowCodec(CATALOG);

        try (var allocator = new RootAllocator();
             var requestRoot = ArrowMessages.newRequestRoot(allocator);
             var batch = ArrowMessages.newResultRoot(allocator);
             var request = new ArrowMessages.SliceReadRequest(USER_KEY_TYPE, requestRoot, new int[]{101, 102})) {
            var output = new ByteArrayOutputStream();
            requestRoot.allocateNew();
            batch.allocateNew();
            writeRow(batch, 0, 0, "userA", 101, intBytes(10));
            writeRow(batch, 1, 0, "userA", 102, intBytes(20));

            try (var writer = codec.newResponseWriter(output)) {
                writer.consume(request, batch);
            }

            assertThat(output.toString(StandardCharsets.UTF_8))
                    .isEqualTo("""
                            [{"key":"user_id","key_value":"userA","features":{"feature1":10,"feature2":20}}]""".trim());
        }
    }

    private static void assertRejected(String json, String message) {
        assertRejected(new LegacyJsonArrowCodec(CATALOG), json, message);
    }

    private static void assertRejected(LegacyJsonArrowCodec codec, String json, String message) {
        var allocator = new RootAllocator();
        try {
            ArrowMessages.ArrowTenantRequest request = null;
            Throwable thrown = null;

            try {
                request = codec.parse(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), allocator);
            } catch (Throwable exception) {
                thrown = exception;
            } finally {
                if (request != null) {
                    request.close();
                }
            }

            if (thrown == null) {
                fail("Expected request rejection for payload: " + json);
            }

            assertThat(thrown)
                    .isInstanceOfSatisfying(ReadRequestException.class, exception -> {
                        assertThat(exception.statusCode()).isEqualTo(400);
                        assertThat(exception).hasMessage(message);
                    });
        } finally {
            allocator.close();
        }
    }

    private static long actualArrowBytes(ArrowMessages.ArrowTenantRequest request) {
        return request.sliceRequests().stream()
                .mapToLong(sliceRequest -> sliceRequest.root().getFieldVectors().stream()
                        .mapToLong(vector -> vector.getBufferSize())
                        .sum() + (long) sliceRequest.featureIds().length * Integer.BYTES)
                .sum();
    }

    private static String requestJson(int keyCount, int featureCount) {
        var builder = new StringBuilder("{\"keys\":[");
        for (int keyIndex = 1; keyIndex <= keyCount; keyIndex++) {
            if (keyIndex > 1) {
                builder.append(',');
            }
            builder.append("{\"user_id\":\"user-").append(keyIndex).append("\"}");
        }
        builder.append("],\"features\":[");
        for (int featureIndex = 1; featureIndex <= featureCount; featureIndex++) {
            if (featureIndex > 1) {
                builder.append(',');
            }
            builder.append("\"feature").append(featureIndex).append("\"");
        }
        return builder.append("]}").toString();
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
