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
    /** Скрывает создание экземпляра приложения. */
    private FeatureStoreApplication() {
    }

    /** Запускает приложение и HTTP-сервер. */
    public static void main(String[] args) throws Exception {
        var httpPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        var cassandraHost = System.getenv().getOrDefault("CASSANDRA_HOST", "127.0.0.1");
        var cassandraPort = Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_PORT", "9042"));
        try (var allocator = new RootAllocator();
             var session = createSession(cassandraHost, cassandraPort);
             var server = createServer(httpPort, allocator, session, cassandraHost, cassandraPort, FeatureCatalogDefaults.create())) {
            server.start();
            Thread.currentThread().join();
        }
    }

    /** Собирает зависимости и создает HTTP-сервер. */
    public static FeatureStoreHttpServer createServer(
            int httpPort,
            RootAllocator allocator,
            CqlSession session,
            String cassandraHost,
            int cassandraPort,
            FeatureCatalog catalog
    ) throws IOException {
        var codec = new LegacyJsonArrowCodec(catalog);
        var accessController = new TenantAccessController(catalog);
        var sliceReadPipe = new CassandraSliceReadPipe(cassandraHost, cassandraPort);
        var readService = new LegacyReadService(allocator, codec, accessController, sliceReadPipe);
        return FeatureStoreHttpServer.start(httpPort, readService);
    }

    /** Создает Cassandra-сессию из переменных окружения. */
    private static CqlSession createSession(String host, int port) {
        return CqlSessionFactory.create(
                host,
                port,
                System.getenv().getOrDefault("CASSANDRA_DATACENTER", "datacenter1"));
    }
}
