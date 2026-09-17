package com.github.alexeyklimov.featurestore.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ResultBatchConsumer;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.SliceReadRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.memory.BufferAllocator;

public final class CassandraSliceReadPipe {
    private static final int PROTOCOL_V4 = 4;
    private static final int DEFAULT_BATCH_SIZE = 512;
    private static final int HEADER_SIZE = 9;
    private static final int RESULT_OPCODE = 0x08;
    private static final int ERROR_OPCODE = 0x00;
    private static final int DEFAULT_SERIAL_CONSISTENCY = 8;

    private final InetSocketAddress cassandraAddress;
    private final int batchSize;
    private final PreparedBlobRowsArrowDecoder optimizedDecoder;

    /** Создает пайп чтения с размером батча по умолчанию. */
    public CassandraSliceReadPipe(String host, int port) {
        this(new InetSocketAddress(host, port), DEFAULT_BATCH_SIZE);
    }

    /** Инициализирует пайп чтения с размером батча. */
    public CassandraSliceReadPipe(String host, int port, int batchSize) {
        this(new InetSocketAddress(host, port), batchSize);
    }

    CassandraSliceReadPipe(InetSocketAddress cassandraAddress, int batchSize) {
        this.cassandraAddress = cassandraAddress;
        this.batchSize = batchSize;
        this.optimizedDecoder = new PreparedBlobRowsArrowDecoder(java.util.List.of("value"));
    }

    /** Читает срезы из Cassandra и отдает их батчами. */
    public void stream(SliceReadRequest request, BufferAllocator allocator, ResultBatchConsumer consumer) throws Exception {
        if (request.featureIds().length == 0) {
            return;
        }
        try (var client = NativeCassandraClient.connect(cassandraAddress)) {
            for (int index = 0; index < request.rowCount(); index++) {
                var entity = request.entity(index);
                var query = queryFor(request, entity);
                var frame = client.execute(query, request.featureIds().length, batchSize);
                try (var batch = optimizedDecoder.decodeFeatureValueRows(frame, allocator, request.requestOrdinal(index), entity)) {
                    if (batch.root().getRowCount() > 0) {
                        consumer.accept(request, batch.root());
                    }
                } finally {
                    frame.release();
                }
            }
        }
    }

    /** Собирает CQL-запрос для сущности и набора фичей. */
    private static String queryFor(SliceReadRequest request, byte[] entity) {
        Term[] features = Arrays.stream(request.featureIds())
                .mapToObj(QueryBuilder::literal)
                .toArray(Term[]::new);
        return QueryBuilder.selectFrom(request.keyType().slice())
                .columns("feature_id", "value")
                .whereColumn("key_id").isEqualTo(QueryBuilder.literal(request.keyType().keyId()))
                .whereColumn("entity").isEqualTo(QueryBuilder.literal(ByteBuffer.wrap(entity)))
                .whereColumn("feature_id").in(features)
                .build()
                .getQuery();
    }

    private static final class NativeCassandraClient implements AutoCloseable {
        private static final FrameCodec<ByteBuf> FRAME_CODEC =
                FrameCodec.defaultClient(new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT), Compressor.none());

        private final Socket socket;
        private final InputStream input;
        private final OutputStream output;
        private short nextStreamId;

        private NativeCassandraClient(Socket socket) throws IOException {
            this.socket = socket;
            this.input = socket.getInputStream();
            this.output = socket.getOutputStream();
            startup();
        }

        static NativeCassandraClient connect(InetSocketAddress address) throws IOException {
            var socket = new Socket();
            socket.connect(address, 5_000);
            socket.setSoTimeout(5_000);
            return new NativeCassandraClient(socket);
        }

        ByteBuf execute(String query, int requestedRows, int maxRows) throws IOException {
            var options = new QueryOptions(
                    ConsistencyLevel.ONE.getProtocolCode(),
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    true,
                    normalizedPageSize(requestedRows, maxRows),
                    null,
                    DEFAULT_SERIAL_CONSISTENCY,
                    QueryOptions.NO_DEFAULT_TIMESTAMP,
                    null,
                    QueryOptions.NO_NOW_IN_SECONDS);
            send(new Query(query, options));
            var frame = readFrame();
            if (opcode(frame) == ERROR_OPCODE) {
                throw decodeError(frame);
            }
            int opcode = opcode(frame);
            if (opcode != RESULT_OPCODE) {
                frame.release();
                throw new IOException("Unexpected Cassandra response opcode: " + opcode);
            }
            return frame;
        }

        private void startup() throws IOException {
            send(new Startup());
            var frame = readFrame();
            try {
                var decoded = FRAME_CODEC.decode(frame);
                if (!(decoded.message instanceof Ready)) {
                    throw new IOException("Native Cassandra client expected READY during startup");
                }
            } finally {
                frame.release();
            }
        }

        private void send(Message message) throws IOException {
            var frame = FRAME_CODEC.encode(Frame.forRequest(PROTOCOL_V4, nextStreamId++, false, Frame.NO_PAYLOAD, message));
            try {
                byte[] bytes = new byte[frame.readableBytes()];
                frame.getBytes(frame.readerIndex(), bytes);
                output.write(bytes);
                output.flush();
            } finally {
                frame.release();
            }
        }

        private ByteBuf readFrame() throws IOException {
            byte[] header = readFully(HEADER_SIZE);
            int bodyLength = ByteBuffer.wrap(header, 5, Integer.BYTES).getInt();
            byte[] body = readFully(bodyLength);
            var frame = UnpooledByteBufAllocator.DEFAULT.directBuffer(HEADER_SIZE + bodyLength);
            frame.writeBytes(header);
            frame.writeBytes(body);
            return frame;
        }

        private IOException decodeError(ByteBuf frame) {
            try {
                var decoded = FRAME_CODEC.decode(frame);
                if (decoded.message instanceof Error error) {
                    return new IOException("Cassandra query failed: " + error.message);
                }
                return new IOException("Cassandra query failed with opcode 0");
            } finally {
                frame.release();
            }
        }

        private byte[] readFully(int length) throws IOException {
            var bytes = input.readNBytes(length);
            if (bytes.length != length) {
                throw new EOFException("Expected %s bytes from Cassandra, got %s".formatted(length, bytes.length));
            }
            return bytes;
        }

        private static int opcode(ByteBuf frame) {
            return frame.getUnsignedByte(4);
        }

        private static int normalizedPageSize(int requestedRows, int maxRows) {
            return Math.max(1, Math.min(maxRows, requestedRows));
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
