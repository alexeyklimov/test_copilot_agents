package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.cassandra.CassandraSliceReadPipe;
import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.testsupport.PerformanceFixtures;
import com.github.alexeyklimov.featurestore.testsupport.TestEnvironment;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ReadPathComponentBenchmark {
    @Benchmark
    public int parseLegacyRequest(ParseState state) throws Exception {
        try (var request = state.codec.parse(new ByteArrayInputStream(state.requestBytes), state.allocator)) {
            return request.sliceRequests().size();
        }
    }

    @Benchmark
    public int authorizeTenantRequest(AuthorizeState state) {
        var authorized = state.accessController.authorize(PerformanceFixtures.TENANT_ID, state.request);
        try {
            state.request = null;
            return authorized.request().sliceRequests().size();
        } finally {
            authorized.close();
        }
    }

    @Benchmark
    public int streamSingleSliceFromCassandra(CassandraState state) throws Exception {
        var rowCount = new int[1];
        state.pipe.stream(state.sliceRequest, state.allocator, (request, batch) -> rowCount[0] += batch.getRowCount());
        return rowCount[0];
    }

    @Benchmark
    public int serializeArrowBatchToLegacyJson(SerializeState state) throws Exception {
        var output = new ByteArrayOutputStream(state.expectedBytes);
        try (var writer = state.codec.newResponseWriter(output)) {
            writer.consume(state.sliceRequest, state.batch);
        }
        return output.size();
    }

    @Benchmark
    public int httpEndToEndLegacyRead(HttpState state) throws Exception {
        var response = state.environment.post(PerformanceFixtures.TENANT_ID, state.requestBody);
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Expected 200, got " + response.statusCode());
        }
        return response.body().length();
    }

    @State(Scope.Benchmark)
    public static class ParseState extends RequestState {
        private RootAllocator allocator;
        private LegacyJsonArrowCodec codec;

        @Setup(Level.Trial)
        public void setUpTrial() {
            initializeRequestState();
            allocator = new RootAllocator();
            codec = new LegacyJsonArrowCodec(catalog);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() {
            allocator.close();
        }
    }

    @State(Scope.Benchmark)
    public static class AuthorizeState extends RequestState {
        private RootAllocator allocator;
        private LegacyJsonArrowCodec codec;
        private TenantAccessController accessController;
        private ArrowMessages.ArrowTenantRequest request;

        @Setup(Level.Trial)
        public void setUpTrial() {
            initializeRequestState();
            allocator = new RootAllocator();
            codec = new LegacyJsonArrowCodec(catalog);
            accessController = new TenantAccessController(catalog);
        }

        @Setup(Level.Invocation)
        public void setUpInvocation() throws Exception {
            request = codec.parse(new ByteArrayInputStream(requestBytes), allocator);
        }

        @TearDown(Level.Invocation)
        public void tearDownInvocation() {
            if (request != null) {
                request.close();
                request = null;
            }
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() {
            allocator.close();
        }
    }

    @State(Scope.Benchmark)
    public static class CassandraState extends RequestState {
        @Param({"32", "256"})
        public int valueSizeBytes;

        private TestEnvironment environment;
        private RootAllocator allocator;
        private CassandraSliceReadPipe pipe;
        private ArrowMessages.ArrowTenantRequest request;
        private ArrowMessages.SliceReadRequest sliceRequest;

        @Setup(Level.Trial)
        public void setUpTrial() throws Exception {
            initializeRequestState();
            environment = TestEnvironment.start(catalog);
            PerformanceFixtures.primeScenario(
                    environment,
                    sliceCount,
                    entitiesPerSlice,
                    featuresPerSlice,
                    entitySizeBytes,
                    valueSizeBytes,
                    PerformanceFixtures.FailureMode.NONE,
                    -1,
                    java.time.Duration.ZERO);
            pipe = new CassandraSliceReadPipe(environment.session());
            allocator = new RootAllocator();
            var codec = new LegacyJsonArrowCodec(catalog);
            request = codec.parse(new ByteArrayInputStream(requestBytes), allocator);
            sliceRequest = request.sliceRequests().get(0);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() throws Exception {
            request.close();
            allocator.close();
            environment.close();
        }
    }

    @State(Scope.Benchmark)
    public static class SerializeState extends RequestState {
        @Param({"32", "256"})
        public int valueSizeBytes;

        private LegacyJsonArrowCodec codec;
        private ArrowMessages.ArrowTenantRequest request;
        private ArrowMessages.SliceReadRequest sliceRequest;
        private RootAllocator requestAllocator;
        private RootAllocator batchAllocator;
        private VectorSchemaRoot batch;
        private int expectedBytes;

        @Setup(Level.Trial)
        public void setUpTrial() throws Exception {
            initializeRequestState();
            codec = new LegacyJsonArrowCodec(catalog);
            requestAllocator = new RootAllocator();
            request = codec.parse(new ByteArrayInputStream(requestBytes), requestAllocator);
            sliceRequest = request.sliceRequests().get(0);
            batchAllocator = new RootAllocator();
            batch = ArrowMessages.newResultRoot(batchAllocator);
            batch.allocateNew();
            fillBatch();
            expectedBytes = Math.max(256, entitiesPerSlice * featuresPerSlice * valueSizeBytes);
        }

        private void fillBatch() {
            var ordinals = (BigIntVector) batch.getVector("request_ordinal");
            var entities = (VarBinaryVector) batch.getVector("entity");
            var featureIds = (IntVector) batch.getVector("feature_id");
            var values = (VarBinaryVector) batch.getVector("value");
            var firstSliceFeatureIds = PerformanceFixtures.featureIds(1, featuresPerSlice);
            var row = 0;
            for (int entityIndex = 1; entityIndex <= entitiesPerSlice; entityIndex++) {
                var entity = PerformanceFixtures.entityValue(1, entityIndex, entitySizeBytes).getBytes(StandardCharsets.UTF_8);
                for (int featureIndex = 1; featureIndex <= featuresPerSlice; featureIndex++) {
                    ordinals.setSafe(row, entityIndex - 1L);
                    entities.setSafe(row, entity);
                    featureIds.setSafe(row, firstSliceFeatureIds[featureIndex - 1]);
                    values.setSafe(
                            row,
                            ("slice1-feature" + featureIndex + "-").repeat(Math.max(1, valueSizeBytes / 16 + 1))
                                    .substring(0, valueSizeBytes)
                                    .getBytes(StandardCharsets.UTF_8));
                    row++;
                }
            }
            batch.setRowCount(row);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() {
            request.close();
            batch.close();
            batchAllocator.close();
            requestAllocator.close();
        }
    }

    @State(Scope.Benchmark)
    public static class HttpState extends RequestState {
        @Param({"32", "256"})
        public int valueSizeBytes;

        private TestEnvironment environment;

        @Setup(Level.Trial)
        public void setUpTrial() throws Exception {
            initializeRequestState();
            environment = TestEnvironment.start(catalog);
            PerformanceFixtures.primeScenario(
                    environment,
                    sliceCount,
                    entitiesPerSlice,
                    featuresPerSlice,
                    entitySizeBytes,
                    valueSizeBytes,
                    PerformanceFixtures.FailureMode.NONE,
                    -1,
                    java.time.Duration.ZERO);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() throws Exception {
            environment.close();
        }
    }

    @State(Scope.Benchmark)
    public abstract static class RequestState {
        @Param({"1", "2"})
        public int sliceCount;

        @Param({"1", "25"})
        public int entitiesPerSlice;

        @Param({"4", "32"})
        public int featuresPerSlice;

        @Param({"16", "128"})
        public int entitySizeBytes;

        protected FeatureCatalog catalog;
        protected String requestBody;
        protected byte[] requestBytes;

        protected void initializeRequestState() {
            catalog = PerformanceFixtures.multiSliceCatalog(sliceCount, featuresPerSlice, FeatureCatalog.ValueEncoding.UTF8);
            requestBody = PerformanceFixtures.requestBody(sliceCount, entitiesPerSlice, featuresPerSlice, entitySizeBytes);
            requestBytes = requestBody.getBytes(StandardCharsets.UTF_8);
        }
    }
}
