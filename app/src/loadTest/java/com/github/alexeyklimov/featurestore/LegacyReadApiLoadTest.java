package com.github.alexeyklimov.featurestore;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.alexeyklimov.featurestore.testsupport.TestCatalogs;
import com.github.alexeyklimov.featurestore.testsupport.TestEnvironment;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class LegacyReadApiLoadTest {
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
                assertThat(response.body()).contains("\"key_value\":\"user-100\",\"features\":{\"feature1\":"
                        + TestEnvironment.pseudoRandomValue("user-100", 1001)
                        + ",");
                assertThat(response.body()).contains("\"feature600\":"
                        + TestEnvironment.pseudoRandomValue("user-100", 1600));
            }
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
}
