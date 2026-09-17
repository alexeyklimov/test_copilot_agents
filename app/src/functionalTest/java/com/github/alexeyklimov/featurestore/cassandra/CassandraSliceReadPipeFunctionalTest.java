package com.github.alexeyklimov.featurestore.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.alexeyklimov.featurestore.model.FeatureCatalogDefaults;
import com.github.alexeyklimov.featurestore.service.LegacyJsonArrowCodec;
import com.github.alexeyklimov.featurestore.testsupport.TestEnvironment;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.junit.jupiter.api.Test;

class CassandraSliceReadPipeFunctionalTest {
    @Test
    void streamsSimulacronRowsThroughIntegratedOptimizedDecoder() throws Exception {
        try (var environment = TestEnvironment.start(FeatureCatalogDefaults.create());
             var allocator = new RootAllocator()) {
            environment.primeRows(
                    TestEnvironment.readQuery("user_features", 1, "userA", 101, 102),
                    Map.of(101, 11, 102, 22));

            var codec = new LegacyJsonArrowCodec(FeatureCatalogDefaults.create());
            try (var request = codec.parse(new ByteArrayInputStream("""
                    {
                      "keys": [{"user_id": "userA"}],
                      "features": ["feature1", "feature2"]
                    }
                    """.getBytes(StandardCharsets.UTF_8)), allocator)) {
                var sliceRequest = request.sliceRequests().get(0);
                var pipe = new CassandraSliceReadPipe(environment.cassandraAddress().getHostString(), environment.cassandraAddress().getPort());

                var batches = new int[1];
                pipe.stream(sliceRequest, allocator, (ignoredRequest, batch) -> {
                    batches[0]++;
                    assertThat(batch.getRowCount()).isEqualTo(2);
                    assertThat(batch.getVector("value")).isInstanceOf(ViewVarBinaryVector.class);
                    assertThat(((BigIntVector) batch.getVector("request_ordinal")).get(0)).isEqualTo(0L);
                    assertThat(new String(((VarBinaryVector) batch.getVector("entity")).get(0), StandardCharsets.UTF_8)).isEqualTo("userA");
                    var featureIds = (IntVector) batch.getVector("feature_id");
                    var values = (ViewVarBinaryVector) batch.getVector("value");
                    var decoded = new LinkedHashMap<Integer, Integer>();
                    for (int row = 0; row < batch.getRowCount(); row++) {
                        decoded.put(featureIds.get(row), intValue(values.get(row)));
                    }
                    assertThat(decoded).isEqualTo(Map.of(101, 11, 102, 22));
                });
                assertThat(batches[0]).isEqualTo(1);
            }
        }
    }

    private static int intValue(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
    }
}
