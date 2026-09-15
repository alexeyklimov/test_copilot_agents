package com.github.alexeyklimov.featurestore;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.github.alexeyklimov.featurestore.testsupport.TestCatalogs;
import com.github.alexeyklimov.featurestore.testsupport.TestEnvironment;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class LegacyReadApiLoadTest {
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Test
    @Timeout(60)
    void handlesConcurrentHundredBySixHundredRequests() throws Exception {
        try (var environment = TestEnvironment.start(TestCatalogs.heavyCatalog(600));
             var executor = Executors.newFixedThreadPool(8)) {
            var requestBody = heavyRequest();
            var futures = IntStream.range(0, 8)
                    .mapToObj(index -> executor.submit(() -> environment.post("tenant-a", requestBody)))
                    .toList();

            for (var future : futures) {
                HttpResponse<String> response = future.get();
                assertThat(response.statusCode()).isEqualTo(200);
                assertThat(countOccurrences(response.body(), "\"key\":\"user_id\"")).isEqualTo(100);
                assertThat(countOccurrences(response.body(), "\"feature600\":")).isEqualTo(100);
                var entities = responseEntities(response.body());
                assertThat(entities).hasSize(100);
                var user1 = entities.get("user-1");
                var user42 = entities.get("user-42");
                var user100 = entities.get("user-100");
                assertThat(user1).containsEntry("feature1", TestEnvironment.pseudoRandomValue("user-1", 1001));
                assertThat(user1).containsEntry("feature600", TestEnvironment.pseudoRandomValue("user-1", 1600));
                assertThat(user42).containsEntry("feature321", TestEnvironment.pseudoRandomValue("user-42", 1321));
                assertThat(user100).containsEntry("feature1", TestEnvironment.pseudoRandomValue("user-100", 1001));
                assertThat(user100).containsEntry("feature600", TestEnvironment.pseudoRandomValue("user-100", 1600));
            }
        }

        @Test
        void parsesEntityFeaturesFromJsonResponse() throws Exception {
            var entities = responseEntities("""
                    [{"key":"user_id","features":{"feature2":22,"feature1":11},"key_value":"user-1"},{"key":"user_id","key_value":"user-2","features":{"feature1":33}}]
                    """.trim());

            assertThat(entities).containsOnlyKeys("user-1", "user-2");
            assertThat(entities.get("user-1")).containsEntry("feature1", 11).containsEntry("feature2", 22);
            assertThat(entities.get("user-2")).containsEntry("feature1", 33);
        }
    }

    private static String heavyRequest() {
        var builder = new StringBuilder("{\"keys\":[");
        for (int entityIndex = 1; entityIndex <= 100; entityIndex++) {
            if (entityIndex > 1) {
                builder.append(',');
            }
            builder.append("{\"user_id\":\"user-").append(entityIndex).append("\"}");
        }
        builder.append("],\"features\":[");
        for (int featureIndex = 1; featureIndex <= 600; featureIndex++) {
            if (featureIndex > 1) {
                builder.append(',');
            }
            builder.append("\"feature").append(featureIndex).append("\"");
        }
        return builder.append("]}").toString();
    }

    private static int countOccurrences(String body, String needle) {
        var count = 0;
        var offset = 0;
        while ((offset = body.indexOf(needle, offset)) >= 0) {
            count++;
            offset += needle.length();
        }
        return count;
    }

    private static Map<String, Map<String, Integer>> responseEntities(String body) throws IOException {
        var entities = new LinkedHashMap<String, Map<String, Integer>>();
        try (var parser = JSON_FACTORY.createParser(body)) {
            assertThat(parser.nextToken()).isEqualTo(JsonToken.START_ARRAY);
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                assertThat(parser.currentToken()).isEqualTo(JsonToken.START_OBJECT);
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
        assertThat(parser.currentToken()).isEqualTo(JsonToken.START_OBJECT);
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            var featureName = parser.currentName();
            parser.nextToken();
            features.put(featureName, parser.getIntValue());
        }
        return features;
    }
}
