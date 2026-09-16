package com.github.alexeyklimov.featurestore.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.model.FeatureCatalogDefaults;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.SliceReadRequest;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class CassandraSliceReadPipeTest {
    private static final FeatureCatalog.KeyType USER_ID = FeatureCatalogDefaults.create()
            .findKeyType("user_id")
            .orElseThrow();

    @Test
    void flushesWhenConfiguredPageSizeBytesWouldOverflow() throws Exception {
        try (var allocator = new RootAllocator();
             var request = request(USER_ID, "userA", 101, 102)) {
            var pipe = new CassandraSliceReadPipe(
                    session(List.of(row(101, bytes(16)), row(102, bytes(16)))),
                    10,
                    48);
            var batchSizes = new ArrayList<Integer>();

            pipe.stream(request, allocator, (currentRequest, batch) -> batchSizes.add(batch.getRowCount()));

            assertThat(batchSizes).containsExactly(1, 1);
        }
    }

    @Test
    void rejectsRowsLargerThanConfiguredPageSizeBytes() throws Exception {
        try (var allocator = new RootAllocator();
             var request = request(USER_ID, "userA", 101)) {
            var pipe = new CassandraSliceReadPipe(session(List.of(row(101, bytes(16)))), 10, 32);

            assertThatThrownBy(() -> pipe.stream(request, allocator, (currentRequest, batch) -> {
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("configured page size bytes");
        }
    }

    private static SliceReadRequest request(FeatureCatalog.KeyType keyType, String entity, int... featureIds) {
        var entityBytes = entity.getBytes(StandardCharsets.UTF_8);
        var allocator = new RootAllocator();
        var ordinals = new BigIntVector("request_ordinal", allocator);
        var entities = new VarBinaryVector("entity", allocator);
        ordinals.allocateNew(1);
        entities.allocateNew();
        ordinals.setSafe(0, 0L);
        ordinals.setValueCount(1);
        entities.setSafe(0, entityBytes);
        entities.setValueCount(1);
        var root = new VectorSchemaRoot(List.of(ordinals, entities));
        root.setRowCount(1);
        return new SliceReadRequest(keyType, root, featureIds);
    }

    private static CqlSession session(List<Row> rows) {
        return proxy(CqlSession.class, (method, args) -> switch (method.getName()) {
            case "execute" -> resultSet(rows);
            case "close" -> null;
            default -> defaultValue(method.getReturnType());
        });
    }

    private static ResultSet resultSet(List<Row> rows) {
        return proxy(ResultSet.class, (method, args) -> switch (method.getName()) {
            case "iterator" -> rows.iterator();
            default -> defaultValue(method.getReturnType());
        });
    }

    private static Row row(int featureId, byte[] value) {
        return proxy(Row.class, (method, args) -> switch (method.getName()) {
            case "getInt" -> featureId;
            case "getByteBuffer" -> ByteBuffer.wrap(value);
            default -> defaultValue(method.getReturnType());
        });
    }

    private static byte[] bytes(int length) {
        return new byte[length];
    }

    private static Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == byte.class) {
            return (byte) 0;
        }
        if (returnType == short.class) {
            return (short) 0;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == float.class) {
            return 0F;
        }
        if (returnType == double.class) {
            return 0D;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> invocation.invoke(method, args));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(java.lang.reflect.Method method, Object[] args) throws Throwable;
    }
}
