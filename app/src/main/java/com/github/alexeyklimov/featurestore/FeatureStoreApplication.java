package com.github.alexeyklimov.featurestore;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.alexeyklimov.featurestore.cassandra.CassandraSliceReadPipe;
import com.github.alexeyklimov.featurestore.cassandra.CqlSessionFactory;
import com.github.alexeyklimov.featurestore.http.FeatureStoreHttpServer;
import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.model.FeatureCatalogDefaults;
import com.github.alexeyklimov.featurestore.service.LegacyJsonArrowCodec;
import com.github.alexeyklimov.featurestore.service.LegacyReadService;
import com.github.alexeyklimov.featurestore.service.TenantAccessController;
import java.io.IOException;
import org.apache.arrow.memory.RootAllocator;

public final class FeatureStoreApplication {
    private FeatureStoreApplication() {
    }

    public static void main(String[] args) throws Exception {
        var httpPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        try (var allocator = new RootAllocator();
             var session = createSession();
             var server = createServer(httpPort, allocator, session, FeatureCatalogDefaults.create())) {
            server.start();
            Thread.currentThread().join();
        }
    }

    public static FeatureStoreHttpServer createServer(
            int httpPort,
            RootAllocator allocator,
            CqlSession session,
            FeatureCatalog catalog
    ) throws IOException {
        var codec = new LegacyJsonArrowCodec(catalog);
        var accessController = new TenantAccessController(catalog);
        var sliceReadPipe = new CassandraSliceReadPipe(session);
        var readService = new LegacyReadService(allocator, codec, accessController, sliceReadPipe);
        return FeatureStoreHttpServer.start(httpPort, readService);
    }

    private static CqlSession createSession() {
        return CqlSessionFactory.create(
                System.getenv().getOrDefault("CASSANDRA_HOST", "127.0.0.1"),
                Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_PORT", "9042")),
                System.getenv().getOrDefault("CASSANDRA_DATACENTER", "datacenter1"));
    }
}
