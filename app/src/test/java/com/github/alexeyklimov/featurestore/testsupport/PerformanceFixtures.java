package com.github.alexeyklimov.featurestore.testsupport;

import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PerformanceFixtures {
    public static final String TENANT_ID = "tenant-a";

    private PerformanceFixtures() {
    }

    public enum FailureMode {
        NONE,
        DELAYED_SUCCESS,
        READ_TIMEOUT,
        SERVER_ERROR
    }

    public static FeatureCatalog multiSliceCatalog(int sliceCount, int featuresPerSlice, FeatureCatalog.ValueEncoding valueEncoding) {
        var keyTypes = new ArrayList<FeatureCatalog.KeyType>(sliceCount);
        var allowedKeyTypes = new java.util.LinkedHashSet<String>();
        for (int sliceIndex = 1; sliceIndex <= sliceCount; sliceIndex++) {
            var features = new FeatureCatalog.FeatureDefinition[featuresPerSlice];
            for (int featureIndex = 1; featureIndex <= featuresPerSlice; featureIndex++) {
                features[featureIndex - 1] = new FeatureCatalog.FeatureDefinition(
                        featureName(featureIndex),
                        featureId(sliceIndex, featureIndex),
                        valueEncoding);
            }
            var keyName = keyName(sliceIndex);
            allowedKeyTypes.add(keyName);
            keyTypes.add(FeatureCatalog.KeyType.of(keyName, sliceIndex, sliceName(sliceIndex), features));
        }
        return new FeatureCatalog(
                keyTypes,
                List.of(new FeatureCatalog.TenantPolicy(
                        TENANT_ID,
                        Set.copyOf(allowedKeyTypes),
                        new FeatureCatalog.RequestQuota(
                                (sliceCount * 1_000) + 10,
                                (sliceCount * featuresPerSlice * 1_000) + 10,
                                256L * 1024 * 1024))));
    }

    public static String requestBody(int sliceCount, int entitiesPerSlice, int featuresPerSlice, int entitySizeBytes) {
        var builder = new StringBuilder("{\"keys\":[");
        var needsComma = false;
        for (int sliceIndex = 1; sliceIndex <= sliceCount; sliceIndex++) {
            for (int entityIndex = 1; entityIndex <= entitiesPerSlice; entityIndex++) {
                if (needsComma) {
                    builder.append(',');
                }
                builder.append("{\"")
                        .append(keyName(sliceIndex))
                        .append("\":\"")
                        .append(entityValue(sliceIndex, entityIndex, entitySizeBytes))
                        .append("\"}");
                needsComma = true;
            }
        }
        builder.append("],\"features\":[");
        for (int featureIndex = 1; featureIndex <= featuresPerSlice; featureIndex++) {
            if (featureIndex > 1) {
                builder.append(',');
            }
            builder.append('"').append(featureName(featureIndex)).append('"');
        }
        return builder.append("]}").toString();
    }

    public static void primeScenario(
            TestEnvironment environment,
            int sliceCount,
            int entitiesPerSlice,
            int featuresPerSlice,
            int entitySizeBytes,
            int valueSizeBytes,
            FailureMode failureMode,
            int affectedSliceIndex,
            Duration delay
    ) {
        for (int sliceIndex = 1; sliceIndex <= sliceCount; sliceIndex++) {
            var values = featureValues(sliceIndex, featuresPerSlice, valueSizeBytes);
            var featureIds = featureIds(sliceIndex, featuresPerSlice);
            for (int entityIndex = 1; entityIndex <= entitiesPerSlice; entityIndex++) {
                var query = TestEnvironment.readQuery(
                        sliceName(sliceIndex),
                        sliceIndex,
                        entityValue(sliceIndex, entityIndex, entitySizeBytes),
                        featureIds);
                if (sliceIndex == affectedSliceIndex) {
                    switch (failureMode) {
                        case NONE -> environment.primeBlobRows(query, values);
                        case DELAYED_SUCCESS -> environment.primeBlobRows(query, values, delay);
                        case READ_TIMEOUT -> environment.primeResult(query, PrimeDsl.readTimeout(ConsistencyLevel.ONE, 1, 0, false), delay);
                        case SERVER_ERROR -> environment.primeResult(query, PrimeDsl.serverError("Injected benchmark failure"), delay);
                    }
                    continue;
                }
                environment.primeBlobRows(query, values);
            }
        }
    }

    public static int[] featureIds(int sliceIndex, int featuresPerSlice) {
        var ids = new int[featuresPerSlice];
        for (int featureIndex = 1; featureIndex <= featuresPerSlice; featureIndex++) {
            ids[featureIndex - 1] = featureId(sliceIndex, featureIndex);
        }
        return ids;
    }

    public static String keyName(int sliceIndex) {
        return "key" + sliceIndex + "_id";
    }

    public static String sliceName(int sliceIndex) {
        return "slice_" + sliceIndex;
    }

    public static String entityValue(int sliceIndex, int entityIndex, int entitySizeBytes) {
        return sizedAscii("slice" + sliceIndex + "-entity" + entityIndex + "-", entitySizeBytes);
    }

    private static Map<Integer, byte[]> featureValues(int sliceIndex, int featuresPerSlice, int valueSizeBytes) {
        var values = new LinkedHashMap<Integer, byte[]>();
        for (int featureIndex = 1; featureIndex <= featuresPerSlice; featureIndex++) {
            values.put(featureId(sliceIndex, featureIndex), utf8Bytes(sliceIndex, featureIndex, valueSizeBytes));
        }
        return values;
    }

    private static byte[] utf8Bytes(int sliceIndex, int featureIndex, int valueSizeBytes) {
        return sizedAscii("slice" + sliceIndex + "-feature" + featureIndex + "-", valueSizeBytes)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static int featureId(int sliceIndex, int featureIndex) {
        return (sliceIndex * 10_000) + featureIndex;
    }

    private static String featureName(int featureIndex) {
        return "feature" + featureIndex;
    }

    private static String sizedAscii(String prefix, int size) {
        var actualSize = Math.max(size, 1);
        if (prefix.length() >= actualSize) {
            return prefix.substring(0, actualSize);
        }
        var builder = new StringBuilder(actualSize);
        builder.append(prefix);
        while (builder.length() < actualSize) {
            builder.append((char) ('a' + Math.floorMod(builder.length(), 26)));
        }
        return builder.toString();
    }
}
