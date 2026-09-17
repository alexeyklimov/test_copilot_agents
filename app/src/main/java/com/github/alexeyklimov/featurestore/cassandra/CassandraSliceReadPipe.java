package com.github.alexeyklimov.featurestore.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ResultBatchConsumer;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ResultTableStreamer;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.SliceReadRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.arrow.memory.BufferAllocator;

public final class CassandraSliceReadPipe {
    private static final int PROTOCOL_V4 = 4;
    private static final int RESULT_OPCODE = 0x08;
    private static final int RESULT_KIND_ROWS = 0x0002;
    private static final int ROWS_NO_METADATA_FLAG = 0x0004;
    private final CqlSession session;
    private final int batchSize;
    private final PreparedBlobRowsArrowDecoder optimizedDecoder;

    /** Создает пайп чтения с размером батча по умолчанию. */
    public CassandraSliceReadPipe(CqlSession session) {
        this(session, 512);
    }

    /** Инициализирует пайп чтения с размером батча. */
    public CassandraSliceReadPipe(CqlSession session, int batchSize) {
        this.session = session;
        this.batchSize = batchSize;
        this.optimizedDecoder = new PreparedBlobRowsArrowDecoder(java.util.List.of("value"));
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
                var statement = queryFor(request, entity);
                var rows = collectRows(session.execute(statement));
                if (streamOptimized(rows, request, request.requestOrdinal(index), entity, allocator, consumer)) {
                    continue;
                }
                for (var row : rows) {
                    streamer.append(
                            request.requestOrdinal(index),
                            entity,
                            row.featureId(),
                            remainingBytes(row.value()),
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

    private boolean streamOptimized(
            java.util.List<FeatureValueRow> rows,
            SliceReadRequest request,
            long requestOrdinal,
            byte[] entity,
            BufferAllocator allocator,
            ResultBatchConsumer consumer
    ) throws Exception {
        var frame = encodeRowsFrame(rows);
        try {
            try (var batch = optimizedDecoder.decodeFeatureValueRows(frame, allocator, requestOrdinal, entity)) {
                if (batch.root().getRowCount() > 0) {
                    consumer.accept(request, batch.root());
                }
                return true;
            }
        } catch (IllegalArgumentException exception) {
            return false;
        } finally {
            frame.release();
        }
    }

    /** Копирует оставшиеся байты из буфера. */
    private static byte[] remainingBytes(ByteBuffer buffer) {
        var copy = buffer.duplicate();
        var bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static java.util.List<FeatureValueRow> collectRows(Iterable<Row> resultSet) {
        var rows = new ArrayList<FeatureValueRow>();
        for (var row : resultSet) {
            var value = row.getByteBuffer("value");
            rows.add(new FeatureValueRow(row.getInt("feature_id"), value == null ? null : value.duplicate()));
        }
        return rows;
    }

    private static ByteBuf encodeRowsFrame(java.util.List<FeatureValueRow> rows) {
        var frame = Unpooled.directBuffer();
        frame.writeZero(9);
        frame.writeInt(RESULT_KIND_ROWS);
        frame.writeInt(ROWS_NO_METADATA_FLAG);
        frame.writeInt(2);
        frame.writeInt(rows.size());
        for (var row : rows) {
            frame.writeInt(Integer.BYTES);
            frame.writeInt(row.featureId());
            var value = row.value();
            if (value == null) {
                frame.writeInt(-1);
            } else {
                var copy = value.duplicate();
                frame.writeInt(copy.remaining());
                frame.writeBytes(copy);
            }
        }
        frame.setByte(0, 0x80 | PROTOCOL_V4);
        frame.setByte(1, 0);
        frame.setShort(2, 0);
        frame.setByte(4, RESULT_OPCODE);
        frame.setInt(5, frame.writerIndex() - 9);
        return frame;
    }

    private record FeatureValueRow(int featureId, ByteBuffer value) {
    }
}
