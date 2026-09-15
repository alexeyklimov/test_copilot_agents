package com.github.alexeyklimov.featurestore.testsupport;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.common.stubbing.StubMapping;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.BoundDataCenter;
import com.datastax.oss.simulacron.server.Server;
import com.github.alexeyklimov.featurestore.FeatureStoreApplication;
import com.github.alexeyklimov.featurestore.http.FeatureStoreHttpServer;
import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.regex.Pattern;
import org.apache.arrow.memory.RootAllocator;

public final class TestEnvironment implements AutoCloseable {
    private static final String READ_QUERY_PREFIX = "SELECT feature_id, value FROM ";
    private static final Pattern FEATURE_IDS_PATTERN = Pattern.compile(" AND feature_id IN \\(([^)]+)\\)$");
    private static final LinkedHashMap<String, String> READ_COLUMN_TYPES = readColumnTypes();

    private final Server simulacron;
    private final BoundNode node;
    private final RootAllocator allocator;
    private final CqlSession session;
    private final FeatureStoreHttpServer server;
    private final HttpClient client;

    private TestEnvironment(
            Server simulacron,
            BoundNode node,
            RootAllocator allocator,
            CqlSession session,
            FeatureStoreHttpServer server
    ) {
        this.simulacron = simulacron;
        this.node = node;
        this.allocator = allocator;
        this.session = session;
        this.server = server;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public static TestEnvironment start(FeatureCatalog catalog) throws Exception {
        var simulacron = Server.builder().build();
        var node = simulacron.register(NodeSpec.builder().build());
        ((BoundDataCenter) node.getDataCenter()).getStubStore().register(new PseudoRandomQueryPrime());
        var allocator = new RootAllocator();
        var address = (InetSocketAddress) node.getAddress();
        waitUntilListening(address);
        var session = CqlSession.builder()
                .addContactPoint(address)
                .withLocalDatacenter("dummy")
                .build();
        var server = FeatureStoreApplication.createServer(0, allocator, session, catalog);
        server.start();
        return new TestEnvironment(simulacron, node, allocator, session, server);
    }

    public void primeRows(String query, Map<Integer, Integer> featureValues) {
        var rows = PrimeDsl.rows().columnTypes("feature_id", "int", "value", "blob");
        for (var entry : featureValues.entrySet()) {
            rows.row("feature_id", entry.getKey(), "value", ByteBuffer.wrap(intBytes(entry.getValue())));
        }
        prime(PrimeDsl.when(query).then(rows).build());
    }

    public void primeNoRows(String query) {
        prime(PrimeDsl.when(query).then(PrimeDsl.noRows()).build());
    }

    public HttpResponse<String> post(String tenantId, String body) throws IOException, InterruptedException {
        return client.send(
                HttpRequest.newBuilder(uri("/read"))
                        .header("Content-Type", "application/json")
                        .header("X-Tenant-Id", tenantId)
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    public URI uri(String path) {
        return URI.create("http://127.0.0.1:" + server.port() + path);
    }

    @Override
    public void close() throws Exception {
        server.close();
        session.close();
        allocator.close();
        node.close();
        simulacron.close();
    }

    public static String readQuery(String slice, int keyId, String entity, int... featureIds) {
        var features = new StringBuilder();
        for (int index = 0; index < featureIds.length; index++) {
            if (index > 0) {
                features.append(", ");
            }
            features.append(featureIds[index]);
        }
        return "SELECT feature_id, value FROM " + slice
                + " WHERE key_id = " + keyId
                + " AND entity = " + blobLiteral(entity.getBytes(StandardCharsets.UTF_8))
                + " AND feature_id IN (" + features + ")";
    }

    public static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }

    private void prime(Prime prime) {
        node.prime(prime);
    }

    private static String blobLiteral(byte[] bytes) {
        var builder = new StringBuilder("0x");
        for (byte value : bytes) {
            builder.append(Character.forDigit((value >> 4) & 0xF, 16));
            builder.append(Character.forDigit(value & 0xF, 16));
        }
        return builder.toString();
    }

    private static void waitUntilListening(InetSocketAddress address) throws IOException, InterruptedException {
        IOException lastError = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try (var socket = new Socket()) {
                socket.connect(address, 250);
                return;
            } catch (IOException exception) {
                lastError = exception;
                Thread.sleep(100);
            }
        }
        throw lastError;
    }

    private static LinkedHashMap<String, String> readColumnTypes() {
        var columnTypes = new LinkedHashMap<String, String>();
        columnTypes.put("feature_id", "int");
        columnTypes.put("value", "blob");
        return columnTypes;
    }

    private static final class PseudoRandomQueryPrime extends StubMapping {
        @Override
        public boolean matches(com.datastax.oss.protocol.internal.Frame frame) {
            if (!(frame.message instanceof com.datastax.oss.protocol.internal.request.Query query)) {
                return false;
            }
            return query.query.startsWith(READ_QUERY_PREFIX) && FEATURE_IDS_PATTERN.matcher(query.query).find();
        }

        @Override
        public List<com.datastax.oss.simulacron.common.stubbing.Action> getActions(
                com.datastax.oss.simulacron.common.cluster.AbstractNode node,
                com.datastax.oss.protocol.internal.Frame frame
        ) {
            var query = (com.datastax.oss.protocol.internal.request.Query) frame.message;
            var rows = new ArrayList<LinkedHashMap<String, Object>>();
            var random = new SplittableRandom(Integer.toUnsignedLong(query.query.hashCode()));
            for (int featureId : featureIds(query.query)) {
                var row = new LinkedHashMap<String, Object>();
                row.put("feature_id", featureId);
                row.put("value", ByteBuffer.wrap(intBytes(1 + Math.floorMod(random.nextInt() ^ featureId, 10_000))));
                rows.add(row);
            }
            return new SuccessResult(rows, READ_COLUMN_TYPES).toActions(node, frame);
        }

        private static int[] featureIds(String query) {
            var matcher = FEATURE_IDS_PATTERN.matcher(query);
            if (!matcher.find()) {
                return new int[0];
            }
            return java.util.Arrays.stream(matcher.group(1).split(",\\s*"))
                    .mapToInt(Integer::parseInt)
                    .toArray();
        }
    }
}
