package com.github.alexeyklimov.featurestore;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.testsupport.PerformanceFixtures;
import com.github.alexeyklimov.featurestore.testsupport.TestEnvironment;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class HeapSaturationBenchmarkMain {
    private HeapSaturationBenchmarkMain() {
    }

    public static void main(String[] args) throws Exception {
        var sliceCount = Integer.getInteger("heap.sliceCount", 2);
        var entitiesPerSlice = Integer.getInteger("heap.entitiesPerSlice", 48);
        var featuresPerSlice = Integer.getInteger("heap.featuresPerSlice", 32);
        var entitySizeBytes = Integer.getInteger("heap.entitySizeBytes", 128);
        var valueSizeBytes = Integer.getInteger("heap.valueSizeBytes", 1024);
        var maxConcurrency = Integer.getInteger("heap.maxConcurrency", 24);
        var concurrencyStep = Integer.getInteger("heap.concurrencyStep", 2);
        var requestsPerWorker = Integer.getInteger("heap.requestsPerWorker", 12);

        var catalog = PerformanceFixtures.multiSliceCatalog(sliceCount, featuresPerSlice, FeatureCatalog.ValueEncoding.UTF8);
        var requestBody = PerformanceFixtures.requestBody(sliceCount, entitiesPerSlice, featuresPerSlice, entitySizeBytes);

        ExecutorService executor = Executors.newFixedThreadPool(maxConcurrency);
        try (var environment = TestEnvironment.start(catalog)) {
            PerformanceFixtures.primeScenario(
                    environment,
                    sliceCount,
                    entitiesPerSlice,
                    featuresPerSlice,
                    entitySizeBytes,
                    valueSizeBytes,
                    PerformanceFixtures.FailureMode.NONE,
                    -1,
                    Duration.ZERO);

            Sample lastSuccessful = null;
            for (int concurrency = 1; concurrency <= maxConcurrency; concurrency += Math.max(1, concurrencyStep)) {
                var sample = runWindow(environment, requestBody, executor, concurrency, requestsPerWorker);
                System.out.printf(
                        "heap-benchmark concurrency=%d successes=%d failures=%d elapsedMs=%d throughputOpsPerSec=%.2f%n",
                        sample.concurrency(),
                        sample.successes(),
                        sample.failures(),
                        sample.elapsed().toMillis(),
                        sample.throughputOpsPerSecond());
                if (!sample.success()) {
                    emitResult(sample, lastSuccessful);
                    return;
                }
                lastSuccessful = sample;
            }

            emitResult(new Sample(maxConcurrency, 0, 0, Duration.ZERO, 0.0, true), lastSuccessful);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static Sample runWindow(
            TestEnvironment environment,
            String requestBody,
            ExecutorService executor,
            int concurrency,
            int requestsPerWorker
    ) throws InterruptedException {
        var futures = new ArrayList<java.util.concurrent.Future<WorkerResult>>(concurrency);
        var startedAt = System.nanoTime();
        for (int worker = 0; worker < concurrency; worker++) {
            futures.add(executor.submit(() -> sendRequests(environment, requestBody, requestsPerWorker)));
        }

        var successes = 0;
        var failures = 0;
        for (var future : futures) {
            try {
                var result = future.get();
                successes += result.successes();
                failures += result.failures();
            } catch (ExecutionException exception) {
                failures += requestsPerWorker;
            }
        }
        var elapsed = Duration.ofNanos(System.nanoTime() - startedAt);
        var throughput = successes / Math.max(elapsed.toNanos() / 1_000_000_000.0, 0.001);
        return new Sample(concurrency, successes, failures, elapsed, throughput, failures == 0);
    }

    private static WorkerResult sendRequests(TestEnvironment environment, String requestBody, int requestsPerWorker) {
        var successes = 0;
        var failures = 0;
        for (int requestIndex = 0; requestIndex < requestsPerWorker; requestIndex++) {
            try {
                var response = environment.post(PerformanceFixtures.TENANT_ID, requestBody);
                if (response.statusCode() == 200) {
                    successes++;
                } else {
                    failures++;
                }
            } catch (Exception exception) {
                failures++;
            }
        }
        return new WorkerResult(successes, failures);
    }

    private static void emitResult(Sample saturationPoint, Sample lastSuccessful) {
        var heapMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        if (lastSuccessful == null) {
            System.out.printf(
                    "HEAP_SATURATION_RESULT heapMb=%d saturatedAtConcurrency=%d bestThroughputOpsPerSec=0.00%n",
                    heapMb,
                    saturationPoint.concurrency());
            return;
        }
        System.out.printf(
                "HEAP_SATURATION_RESULT heapMb=%d saturatedAtConcurrency=%d bestStableConcurrency=%d bestThroughputOpsPerSec=%.2f%n",
                heapMb,
                saturationPoint.concurrency(),
                lastSuccessful.concurrency(),
                lastSuccessful.throughputOpsPerSecond());
    }

    private record WorkerResult(int successes, int failures) {
    }

    private record Sample(
            int concurrency,
            int successes,
            int failures,
            Duration elapsed,
            double throughputOpsPerSecond,
            boolean success
    ) {
    }
}
