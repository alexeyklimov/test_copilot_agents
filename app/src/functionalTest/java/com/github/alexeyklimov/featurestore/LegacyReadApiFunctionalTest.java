package com.github.alexeyklimov.featurestore;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.github.alexeyklimov.featurestore.model.FeatureCatalogDefaults;
import com.github.alexeyklimov.featurestore.testsupport.TestEnvironment;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LegacyReadApiFunctionalTest {
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

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
            var expected = new LinkedHashMap<String, Map<String, Integer>>();
            expected.put("userA", Map.of("feature1", 1, "feature2", 2, "feature3", 3));
            expected.put("userB", Map.of("feature1", 4, "feature2", 5, "feature3", 6));
            expected.put("carA", Map.of("feature4", 10));
            assertThat(responseEntities(response.body())).containsExactlyEntriesOf(expected);
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

    private static Map<String, Map<String, Integer>> responseEntities(String body) throws IOException {
        var entities = new LinkedHashMap<String, Map<String, Integer>>();
        try (var parser = JSON_FACTORY.createParser(body)) {
            require(parser.nextToken() == JsonToken.START_ARRAY, "Response must start with a JSON array");
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                require(parser.currentToken() == JsonToken.START_OBJECT, "Each response entry must be a JSON object");
                var keyValue = "";
                Map<String, Integer> features = Map.of();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    var fieldName = parser.currentName();
                    parser.nextToken();
                    switch (fieldName) {
                        case "key_value" -> keyValue = parser.getValueAsString();
                        case "features" -> features = readFeatures(parser);
                        default -> parser.skipChildren();
                    }
                }
                entities.put(keyValue, features);
            }
        }
        return entities;
    }

    private static Map<String, Integer> readFeatures(com.fasterxml.jackson.core.JsonParser parser) throws IOException {
        var features = new LinkedHashMap<String, Integer>();
        require(parser.currentToken() == JsonToken.START_OBJECT, "'features' must be a JSON object");
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            var featureName = parser.currentName();
            parser.nextToken();
            features.put(featureName, parser.getIntValue());
        }
        return features;
    }

    private static void require(boolean condition, String message) throws IOException {
        if (!condition) {
            throw new IOException(message);
        }
    }
}
