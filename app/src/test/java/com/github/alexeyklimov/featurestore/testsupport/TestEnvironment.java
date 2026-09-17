package com.github.alexeyklimov.featurestore.testsupport;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.simulacron.common.result.Result;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.arrow.memory.RootAllocator;

public final class TestEnvironment implements AutoCloseable {
    private static final Pattern READ_QUERY_PATTERN = Pattern.compile("^SELECT\\s+feature_id\\s*,\\s*value\\s+FROM\\s+.+", Pattern.CASE_INSENSITIVE);
    private static final Pattern KEY_ID_PATTERN = Pattern.compile("\\bWHERE\\s+key_id\\s*=\\s*\\d+\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern ENTITY_PATTERN = Pattern.compile("\\bentity\\s*=\\s*(0x[0-9a-fA-F]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern FEATURE_IDS_PATTERN = Pattern.compile("\\bfeature_id\\s+IN\\s*\\((\\d+(?:\\s*,\\s*\\d+)*)\\)\\s*;?$", Pattern.CASE_INSENSITIVE);
    private static final LinkedHashMap<String, String> READ_COLUMN_TYPES = readColumnTypes();

    private final Server simulacron;
    private final BoundNode node;
    private final RootAllocator allocator;
    private final CqlSession session;
    private final FeatureStoreHttpServer server;
    private final HttpClient client;
    private final Set<String> primedQueries;

    private TestEnvironment(
            Server simulacron,
            BoundNode node,
            RootAllocator allocator,
            CqlSession session,
            FeatureStoreHttpServer server,
            Set<String> primedQueries
    ) {
        this.simulacron = simulacron;
        this.node = node;
        this.allocator = allocator;
        this.session = session;
        this.server = server;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.primedQueries = primedQueries;
    }

    public static TestEnvironment start(FeatureCatalog catalog) throws Exception {
        return start(catalog, null);
    }

    public static TestEnvironment start(FeatureCatalog catalog, Duration requestTimeout) throws Exception {
        var simulacron = Server.builder().build();
        var node = simulacron.register(NodeSpec.builder().build());
        var primedQueries = ConcurrentHashMap.<String>newKeySet();
        fallbackStubStore(node).register(new PseudoRandomQueryPrime(primedQueries));
        var allocator = new RootAllocator();
        var address = (InetSocketAddress) node.getAddress();
        waitUntilListening(address);
        var sessionBuilder = CqlSession.builder()
                .addContactPoint(address)
                .withLocalDatacenter("dummy");
        if (requestTimeout != null) {
            sessionBuilder.withConfigLoader(DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, requestTimeout)
                    .build());
        }
        var session = sessionBuilder.build();
        var server = FeatureStoreApplication.createServer(0, allocator, session, address.getHostString(), address.getPort(), catalog);
        server.start();
        return new TestEnvironment(simulacron, node, allocator, session, server, primedQueries);
    }

    public void primeRows(String query, Map<Integer, Integer> featureValues) {
        primeRows(query, featureValues, Duration.ZERO);
    }

    public void primeRows(String query, Map<Integer, Integer> featureValues, Duration delay) {
        var rows = new LinkedHashMap<Integer, byte[]>();
        for (var entry : featureValues.entrySet()) {
            rows.put(entry.getKey(), intBytes(entry.getValue()));
        }
        primeBlobRows(query, rows, delay);
    }

    public void primeBlobRows(String query, Map<Integer, byte[]> featureValues) {
        primeBlobRows(query, featureValues, Duration.ZERO);
    }

    public void primeBlobRows(String query, Map<Integer, byte[]> featureValues, Duration delay) {
        var rows = PrimeDsl.rows().columnTypes("feature_id", "int", "value", "blob");
        for (var entry : featureValues.entrySet()) {
            rows.row("feature_id", entry.getKey(), "value", ByteBuffer.wrap(entry.getValue()));
        }
        prime(PrimeDsl.when(query).then(rows), delay);
    }

    public void primeNoRows(String query) {
        primeNoRows(query, Duration.ZERO);
    }

    public void primeNoRows(String query, Duration delay) {
        prime(PrimeDsl.when(query).then(PrimeDsl.noRows()), delay);
    }

    public void primeResult(String query, Result result) {
        primeResult(query, result, Duration.ZERO);
    }

    public void primeResult(String query, Result result, Duration delay) {
        prime(PrimeDsl.when(query).then(result), delay);
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

    public CqlSession session() {
        return session;
    }

    public InetSocketAddress cassandraAddress() {
        return (InetSocketAddress) node.getAddress();
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
        Term[] features = java.util.Arrays.stream(featureIds)
                .mapToObj(QueryBuilder::literal)
                .toArray(Term[]::new);
        return QueryBuilder.selectFrom(slice)
                .columns("feature_id", "value")
                .whereColumn("key_id").isEqualTo(QueryBuilder.literal(keyId))
                .whereColumn("entity").isEqualTo(QueryBuilder.literal(ByteBuffer.wrap(entity.getBytes(StandardCharsets.UTF_8))))
                .whereColumn("feature_id").in(features)
                .build()
                .getQuery();
    }

    public static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }

    public static int pseudoRandomValue(String entity, int featureId) {
        return PseudoRandomQueryPrime.pseudoRandomValue(blobLiteral(entity.getBytes(StandardCharsets.UTF_8)), featureId);
    }

    private void prime(Prime prime) {
        if (prime.getPrimedRequest().when instanceof com.datastax.oss.simulacron.common.request.Query query) {
            primedQueries.add(query.query);
        }
        node.prime(prime);
    }

    private void prime(PrimeDsl.PrimeBuilder builder, Duration delay) {
        if (!delay.isZero() && !delay.isNegative()) {
            builder.delay(delay.toMillis(), TimeUnit.MILLISECONDS);
        }
        prime(builder.build());
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

    private static com.datastax.oss.simulacron.server.StubStore fallbackStubStore(BoundNode node) {
        var dataCenter = node.getDataCenter();
        if (dataCenter instanceof BoundDataCenter boundDataCenter) {
            return boundDataCenter.getStubStore();
        }
        return node.getStubStore();
    }

    private static final class PseudoRandomQueryPrime extends StubMapping {
        private final Set<String> primedQueries;

        private PseudoRandomQueryPrime(Set<String> primedQueries) {
            this.primedQueries = primedQueries;
        }

        @Override
        public boolean matches(com.datastax.oss.simulacron.common.cluster.AbstractNode node, com.datastax.oss.protocol.internal.Frame frame) {
            return matches(frame)
                    && frame.message instanceof com.datastax.oss.protocol.internal.request.Query query
                    && !primedQueries.contains(query.query);
        }

        @Override
        public boolean matches(com.datastax.oss.protocol.internal.Frame frame) {
            if (!(frame.message instanceof com.datastax.oss.protocol.internal.request.Query query)) {
                return false;
            }
            return READ_QUERY_PATTERN.matcher(query.query).find()
                    && KEY_ID_PATTERN.matcher(query.query).find()
                    && ENTITY_PATTERN.matcher(query.query).find()
                    && FEATURE_IDS_PATTERN.matcher(query.query).find();
        }

        @Override
        public List<com.datastax.oss.simulacron.common.stubbing.Action> getActions(
                com.datastax.oss.simulacron.common.cluster.AbstractNode node,
                com.datastax.oss.protocol.internal.Frame frame
        ) {
            var query = (com.datastax.oss.protocol.internal.request.Query) frame.message;
            var rows = new ArrayList<LinkedHashMap<String, Object>>();
            var entityLiteral = entityLiteral(query.query);
            for (int featureId : featureIds(query.query)) {
                var row = new LinkedHashMap<String, Object>();
                row.put("feature_id", featureId);
                row.put("value", ByteBuffer.wrap(intBytes(pseudoRandomValue(entityLiteral, featureId))));
                rows.add(row);
            }
            return new SuccessResult(rows, READ_COLUMN_TYPES).toActions(node, frame);
        }

        private static String entityLiteral(String query) {
            var matcher = ENTITY_PATTERN.matcher(query);
            return matcher.find() ? matcher.group(1) : "";
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

        private static int pseudoRandomValue(String entityLiteral, int featureId) {
            return 1 + Math.floorMod((31 * entityLiteral.hashCode()) + featureId, 10_000);
        }
    }
}
