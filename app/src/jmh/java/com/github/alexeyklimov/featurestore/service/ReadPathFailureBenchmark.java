package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.testsupport.PerformanceFixtures;
import com.github.alexeyklimov.featurestore.testsupport.TestEnvironment;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
public class ReadPathFailureBenchmark {
    @Benchmark
    public int httpReadWhenSliceFails(FailureState state) throws Exception {
        var response = state.environment.post(PerformanceFixtures.TENANT_ID, state.requestBody);
        if (response.statusCode() != 500) {
            throw new IllegalStateException("Expected 500, got " + response.statusCode());
        }
        return response.body().length();
    }

    @State(Scope.Benchmark)
    public static class FailureState {
        @Param({"READ_TIMEOUT", "SERVER_ERROR"})
        public String failureModeName;

        @Param({"2"})
        public int sliceCount;

        @Param({"1", "10"})
        public int entitiesPerSlice;

        @Param({"4", "16"})
        public int featuresPerSlice;

        @Param({"16", "128"})
        public int entitySizeBytes;

        @Param({"64"})
        public int valueSizeBytes;

        private TestEnvironment environment;
        private String requestBody;

        @Setup(Level.Trial)
        public void setUpTrial() throws Exception {
            var catalog = PerformanceFixtures.multiSliceCatalog(sliceCount, featuresPerSlice, FeatureCatalog.ValueEncoding.UTF8);
            requestBody = PerformanceFixtures.requestBody(sliceCount, entitiesPerSlice, featuresPerSlice, entitySizeBytes);
            environment = TestEnvironment.start(catalog, Duration.ofMillis(250));
            PerformanceFixtures.primeScenario(
                    environment,
                    sliceCount,
                    entitiesPerSlice,
                    featuresPerSlice,
                    entitySizeBytes,
                    valueSizeBytes,
                    PerformanceFixtures.FailureMode.valueOf(failureModeName),
                    sliceCount,
                    Duration.ZERO);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() throws Exception {
            environment.close();
        }
    }
}
