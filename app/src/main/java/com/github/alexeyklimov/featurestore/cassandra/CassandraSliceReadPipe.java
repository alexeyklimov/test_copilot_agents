package com.github.alexeyklimov.featurestore.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ResultBatchConsumer;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ResultTableStreamer;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.SliceReadRequest;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;

public final class CassandraSliceReadPipe {
    private final CqlSession session;
    private final int batchSize;

    /** Создает пайп чтения с размером батча по умолчанию. */
    public CassandraSliceReadPipe(CqlSession session) {
        this(session, 512);
    }

    /** Инициализирует пайп чтения с размером батча. */
    public CassandraSliceReadPipe(CqlSession session, int batchSize) {
        this.session = session;
        this.batchSize = batchSize;
    }

    /** Читает срезы из Cassandra и отдает их батчами. */
    public void stream(SliceReadRequest request, BufferAllocator allocator, ResultBatchConsumer consumer) throws Exception {
        if (request.featureIds().length == 0) {
            return;
        }
        try (var streamer = new ResultTableStreamer(allocator.newChildAllocator(
                "slice-results-" + request.keyType().name(),
                0,
                Long.MAX_VALUE), batchSize)) {
            for (int index = 0; index < request.rowCount(); index++) {
                var entity = request.entity(index);
                var statement = SimpleStatement.newInstance(queryFor(request, entity));
                var resultSet = session.execute(statement);
                for (var row : resultSet) {
                    streamer.append(
                            request.requestOrdinal(index),
                            entity,
                            row.getInt("feature_id"),
                            remainingBytes(row.getByteBuffer("value")),
                            request,
                            consumer);
                }
            }
            streamer.flush(request, consumer);
        }
    }

    /** Собирает CQL-запрос для сущности и набора фичей. */
    private static String queryFor(SliceReadRequest request, byte[] entity) {
        var features = Arrays.stream(request.featureIds())
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(", "));
        return "SELECT feature_id, value FROM " + request.keyType().slice()
                + " WHERE key_id = " + request.keyType().keyId()
                + " AND entity = " + blobLiteral(entity)
                + " AND feature_id IN (" + features + ")";
    }

    /** Преобразует байты в CQL-литерал blob. */
    private static String blobLiteral(byte[] bytes) {
        var builder = new StringBuilder("0x");
        for (byte value : bytes) {
            builder.append(Character.forDigit((value >> 4) & 0xF, 16));
            builder.append(Character.forDigit(value & 0xF, 16));
        }
        return builder.toString();
    }

    /** Копирует оставшиеся байты из буфера. */
    private static byte[] remainingBytes(ByteBuffer buffer) {
        var copy = buffer.duplicate();
        var bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
