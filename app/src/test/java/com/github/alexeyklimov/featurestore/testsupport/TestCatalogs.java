package com.github.alexeyklimov.featurestore.testsupport;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class TestCatalogs {
    private TestCatalogs() {
    }

    public static FeatureCatalog heavyCatalog(int featureCount) {
        var features = new ArrayList<FeatureCatalog.FeatureDefinition>(featureCount);
        for (int index = 1; index <= featureCount; index++) {
            features.add(new FeatureCatalog.FeatureDefinition(
                    "feature" + index,
                    1000 + index,
                    FeatureCatalog.ValueEncoding.INT32));
        }
        var userType = FeatureCatalog.KeyType.of("user_id", 1, "user_features", features.toArray(FeatureCatalog.FeatureDefinition[]::new));
        return new FeatureCatalog(
                List.of(userType),
                List.of(new FeatureCatalog.TenantPolicy(
                        "tenant-a",
                        Set.of("user_id"),
                        new FeatureCatalog.RequestQuota(500, 200_000, 64L * 1024 * 1024))));
    }
}
