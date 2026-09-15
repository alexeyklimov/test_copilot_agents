package com.github.alexeyklimov.featurestore.model;

import java.util.List;
import java.util.Set;

public final class FeatureCatalogDefaults {
    private FeatureCatalogDefaults() {
    }

    public static FeatureCatalog create() {
        var userId = FeatureCatalog.KeyType.of(
                "user_id",
                1,
                "user_features",
                new FeatureCatalog.FeatureDefinition("feature1", 101, FeatureCatalog.ValueEncoding.INT32),
                new FeatureCatalog.FeatureDefinition("feature2", 102, FeatureCatalog.ValueEncoding.INT32),
                new FeatureCatalog.FeatureDefinition("feature3", 103, FeatureCatalog.ValueEncoding.INT32));
        var carId = FeatureCatalog.KeyType.of(
                "car_id",
                2,
                "car_features",
                new FeatureCatalog.FeatureDefinition("feature4", 201, FeatureCatalog.ValueEncoding.INT32));
        return new FeatureCatalog(
                List.of(userId, carId),
                List.of(
                        new FeatureCatalog.TenantPolicy(
                                "tenant-a",
                                Set.of("user_id", "car_id"),
                                new FeatureCatalog.RequestQuota(200, 10_000, 8L * 1024 * 1024)),
                        new FeatureCatalog.TenantPolicy(
                                "tenant-user-only",
                                Set.of("user_id"),
                                new FeatureCatalog.RequestQuota(200, 10_000, 8L * 1024 * 1024)),
                        new FeatureCatalog.TenantPolicy(
                                "tenant-tight",
                                Set.of("user_id", "car_id"),
                                new FeatureCatalog.RequestQuota(5, 20, 8 * 1024))));
    }
}
