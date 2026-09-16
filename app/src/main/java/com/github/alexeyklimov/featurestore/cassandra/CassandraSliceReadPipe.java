package com.github.alexeyklimov.featurestore.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ResultBatchConsumer;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ResultTableStreamer;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.SliceReadRequest;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.arrow.memory.BufferAllocator;

public final class CassandraSliceReadPipe {
    public static final int DEFAULT_BATCH_SIZE = 512;
    public static final int DEFAULT_PAGE_SIZE_BYTES = 1024 * 1024;

    private final CqlSession session;
    private final int batchSize;
    private final int pageSizeBytes;

    /** Создает пайп чтения с размером батча по умолчанию. */
    public CassandraSliceReadPipe(CqlSession session) {
        this(session, DEFAULT_BATCH_SIZE, DEFAULT_PAGE_SIZE_BYTES);
    }

    /** Инициализирует пайп чтения с размером батча. */
    public CassandraSliceReadPipe(CqlSession session, int batchSize) {
        this(session, batchSize, DEFAULT_PAGE_SIZE_BYTES);
    }

    /** Инициализирует пайп чтения с ограничением размера страницы в байтах. */
    public CassandraSliceReadPipe(CqlSession session, int batchSize, int pageSizeBytes) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (pageSizeBytes <= 0) {
            throw new IllegalArgumentException("pageSizeBytes must be positive");
        }
        this.session = session;
        this.batchSize = batchSize;
        this.pageSizeBytes = pageSizeBytes;
    }

    /** Читает срезы из Cassandra и отдает их батчами. */
    public void stream(SliceReadRequest request, BufferAllocator allocator, ResultBatchConsumer consumer) throws Exception {
        if (request.featureIds().length == 0) {
            return;
        }
        try (var streamer = new ResultTableStreamer(allocator.newChildAllocator(
                "slice-results-" + request.keyType().name(),
                0,
                Long.MAX_VALUE), batchSize, pageSizeBytes)) {
            for (int index = 0; index < request.rowCount(); index++) {
                var entity = request.entity(index);
                var statement = queryFor(request, entity);
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
    private static SimpleStatement queryFor(SliceReadRequest request, byte[] entity) {
        Term[] features = Arrays.stream(request.featureIds())
                .mapToObj(QueryBuilder::literal)
                .toArray(Term[]::new);
        return QueryBuilder.selectFrom(request.keyType().slice())
                .columns("feature_id", "value")
                .whereColumn("key_id").isEqualTo(QueryBuilder.literal(request.keyType().keyId()))
                .whereColumn("entity").isEqualTo(QueryBuilder.literal(ByteBuffer.wrap(entity)))
                .whereColumn("feature_id").in(features)
                .build();
    }

    /** Копирует оставшиеся байты из буфера. */
    private static byte[] remainingBytes(ByteBuffer buffer) {
        var copy = buffer.duplicate();
        var bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
