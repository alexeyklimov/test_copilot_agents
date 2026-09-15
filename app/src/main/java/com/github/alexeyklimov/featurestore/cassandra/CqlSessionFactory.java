package com.github.alexeyklimov.featurestore.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;

public final class CqlSessionFactory {
    private CqlSessionFactory() {
    }

    public static CqlSession create(String host, int port, String datacenter) {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(datacenter)
                .build();
    }
}
