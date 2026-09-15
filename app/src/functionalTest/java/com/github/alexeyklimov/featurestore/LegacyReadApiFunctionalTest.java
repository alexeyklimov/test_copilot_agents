package com.github.alexeyklimov.featurestore;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.alexeyklimov.featurestore.model.FeatureCatalogDefaults;
import com.github.alexeyklimov.featurestore.testsupport.TestEnvironment;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LegacyReadApiFunctionalTest {
    @Test
    void streamsFeaturesFromMultipleSlices() throws Exception {
        try (var environment = TestEnvironment.start(FeatureCatalogDefaults.create())) {
            environment.primeRows(TestEnvironment.readQuery("user_features", 1, "userA", 101, 102, 103), Map.of(101, 1, 102, 2, 103, 3));
            environment.primeRows(TestEnvironment.readQuery("user_features", 1, "userB", 101, 102, 103), Map.of(101, 4, 102, 5, 103, 6));
            environment.primeRows(TestEnvironment.readQuery("car_features", 2, "carA", 201), Map.of(201, 10));
            environment.primeNoRows(TestEnvironment.readQuery("user_features", 1, "userC", 101, 102, 103));
            environment.primeNoRows(TestEnvironment.readQuery("car_features", 2, "carB", 201));
            environment.primeNoRows(TestEnvironment.readQuery("car_features", 2, "carC", 201));

            var response = environment.post(
                    "tenant-a",
                    """
                    {
                      "keys": [
                        {"user_id": "userA"},
                        {"user_id": "userB"},
                        {"user_id": "userC"},
                        {"car_id": "carA"},
                        {"car_id": "carB"},
                        {"car_id": "carC"}
                      ],
                      "features": ["feature1", "feature2", "feature3", "feature4"]
                    }
                    """);

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isEqualTo(
                    """
                    [{"key":"user_id","key_value":"userA","features":{"feature1":1,"feature2":2,"feature3":3}},{"key":"user_id","key_value":"userB","features":{"feature1":4,"feature2":5,"feature3":6}},{"key":"car_id","key_value":"carA","features":{"feature4":10}}]"""
                            .trim());
        }
    }

    @Test
    void rejectsUnauthorizedSliceAccess() throws Exception {
        try (var environment = TestEnvironment.start(FeatureCatalogDefaults.create())) {
            var response = environment.post(
                    "tenant-user-only",
                    """
                    {
                      "keys": [{"car_id": "carA"}],
                      "features": ["feature4"]
                    }
                    """);

            assertThat(response.statusCode()).isEqualTo(403);
            assertThat(response.body()).contains("Tenant cannot access key type: car_id");
        }
    }

    @Test
    void rejectsQuotaOverflowBeforeSliceReads() throws Exception {
        try (var environment = TestEnvironment.start(FeatureCatalogDefaults.create())) {
            var response = environment.post(
                    "tenant-tight",
                    """
                    {
                      "keys": [
                        {"user_id": "userA"},
                        {"user_id": "userB"},
                        {"user_id": "userC"},
                        {"user_id": "userD"},
                        {"user_id": "userE"},
                        {"user_id": "userF"}
                      ],
                      "features": ["feature1", "feature2", "feature3", "feature4"]
                    }
                    """);

            assertThat(response.statusCode()).isEqualTo(429);
            assertThat(response.body()).contains("Tenant quota exceeded");
        }
    }
}
