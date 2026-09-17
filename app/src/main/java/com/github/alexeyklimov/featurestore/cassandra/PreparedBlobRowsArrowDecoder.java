package com.github.alexeyklimov.featurestore.cassandra;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Декодирует узкий ROWS-ответ Cassandra v4 без метаданных прямо в Arrow binary-view векторы.
 * Оптимизированный путь предполагает неподжатый кадр и фиксированную blob-only схему.
 */
public final class PreparedBlobRowsArrowDecoder {
    private static final int FRAME_HEADER_SIZE = 9;
    private static final int RESPONSE_DIRECTION_MASK = 0x80;
    private static final int SUPPORTED_PROTOCOL_VERSION = 4;
    private static final int RESULT_OPCODE = 0x08;
    private static final int RESULT_KIND_ROWS = 0x0002;
    private static final int ROWS_NO_METADATA_FLAG = 0x0004;
    private static final int VIEW_WIDTH_BYTES = 16;
    private static final int INLINE_BINARY_BYTES = 12;

    private final List<String> columnNames;
    private final List<Field> schemaFields;

    /** Создает специализированный декодер для фиксированного набора blob-колонок. */
    public PreparedBlobRowsArrowDecoder(List<String> columnNames) {
        Objects.requireNonNull(columnNames, "columnNames must not be null");
        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException("At least one blob column is required");
        }
        this.columnNames = List.copyOf(columnNames);
        this.schemaFields = this.columnNames.stream()
                .map(name -> new Field(name, FieldType.nullable(ArrowType.Binary.INSTANCE), null))
                .toList();
    }

    /** Декодирует неподжатый ROWS-кадр Cassandra v4 в batch с view-based blob колонками. */
    public DecodedBatch decode(ByteBuf frame, BufferAllocator allocator) {
        Objects.requireNonNull(frame, "frame must not be null");
        Objects.requireNonNull(allocator, "allocator must not be null");

        var retainedFrame = frame.retainedSlice(frame.readerIndex(), frame.readableBytes());
        try {
            var layout = parse(retainedFrame);
            var vectors = new ArrayList<FieldVector>(layout.columns.size());
            try {
                for (int columnIndex = 0; columnIndex < layout.columns.size(); columnIndex++) {
                    vectors.add(buildVector(allocator, schemaFields.get(columnIndex), retainedFrame, layout.rowCount, layout.columns.get(columnIndex)));
                }
                return new DecodedBatch(new VectorSchemaRoot(schemaFields, vectors, layout.rowCount));
            } catch (Throwable throwable) {
                closeAll(vectors);
                throw throwable;
            }
        } finally {
            retainedFrame.release();
        }
    }

    private ParsedLayout parse(ByteBuf frame) {
        require(frame.readableBytes() >= FRAME_HEADER_SIZE, "Malformed frame: missing native protocol header");
        int version = frame.getUnsignedByte(0);
        require((version & RESPONSE_DIRECTION_MASK) != 0 && (version & 0x7F) == SUPPORTED_PROTOCOL_VERSION,
                "Optimized path only supports Cassandra response protocol v4 frames");
        require(frame.getUnsignedByte(1) == 0, "Optimized path only supports uncompressed v4 frames without header flags");
        require(frame.getUnsignedByte(4) == RESULT_OPCODE, "Optimized path only supports RESULT responses");

        int bodyLength = frame.getInt(5);
        require(bodyLength == frame.readableBytes() - FRAME_HEADER_SIZE,
                "Malformed frame: header body length does not match the retained frame size");

        int index = FRAME_HEADER_SIZE;
        require(bodyLength >= 12, "Malformed frame: result body is too short");

        int resultKind = frame.getInt(index);
        index += Integer.BYTES;
        require(resultKind == RESULT_KIND_ROWS, "Optimized path only supports ROWS results");

        int metadataFlags = frame.getInt(index);
        index += Integer.BYTES;
        require(metadataFlags == ROWS_NO_METADATA_FLAG,
                "Optimized path only supports prepared ROWS responses with NO_METADATA");

        int columnCount = frame.getInt(index);
        index += Integer.BYTES;
        require(columnCount == columnNames.size(),
                "Unexpected prepared ROWS column count: expected %s, got %s".formatted(columnNames.size(), columnCount));

        require(index + Integer.BYTES <= frame.readableBytes(), "Malformed frame: missing row count");
        int rowCount = frame.getInt(index);
        index += Integer.BYTES;
        require(rowCount >= 0, "Malformed frame: negative row count");

        var columns = new ArrayList<ColumnLayout>(columnCount);
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            columns.add(new ColumnLayout(rowCount));
        }

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                require(index + Integer.BYTES <= frame.readableBytes(),
                        "Malformed frame: missing blob length for row %s column %s".formatted(rowIndex, columnIndex));
                int length = frame.getInt(index);
                index += Integer.BYTES;

                var column = columns.get(columnIndex);
                if (length == -1) {
                    column.lengths[rowIndex] = -1;
                    column.nullCount++;
                    continue;
                }
                require(length >= 0, "Malformed frame: negative blob length %s".formatted(length));
                require((long) index + length <= frame.readableBytes(),
                        "Malformed frame: blob payload overruns the retained frame");

                column.offsets[rowIndex] = index;
                column.lengths[rowIndex] = length;
                index += length;
            }
        }

        require(index == frame.readableBytes(), "Malformed frame: trailing bytes remain after blob-only row decode");
        return new ParsedLayout(rowCount, columns);
    }

    private static ViewVarBinaryVector buildVector(
            BufferAllocator allocator,
            Field field,
            ByteBuf retainedFrame,
            int rowCount,
            ColumnLayout column
    ) {
        var vector = new ViewVarBinaryVector(field, allocator);
        try (var validity = zeroedBuffer(allocator, validityBufferSize(rowCount));
             var view = zeroedBuffer(allocator, viewBufferSize(rowCount));
             var data = allocator.wrapForeignAllocation(new NettyForeignAllocation(retainedFrame.retainedDuplicate()))) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                int length = column.lengths[rowIndex];
                if (length < 0) {
                    continue;
                }
                setBit(validity, rowIndex);
                int entryOffset = rowIndex * VIEW_WIDTH_BYTES;
                view.setInt(entryOffset, length);
                if (length <= INLINE_BINARY_BYTES) {
                    view.setBytes(entryOffset + Integer.BYTES, data, column.offsets[rowIndex], length);
                } else {
                    view.setBytes(entryOffset + Integer.BYTES, data, column.offsets[rowIndex], Integer.BYTES);
                    view.setInt(entryOffset + (2 * Integer.BYTES), 0);
                    view.setInt(entryOffset + (3 * Integer.BYTES), column.offsets[rowIndex]);
                }
            }
            vector.loadFieldBuffers(new ArrowFieldNode(rowCount, column.nullCount), List.of(validity, view, data));
            return vector;
        } catch (Throwable throwable) {
            vector.close();
            throw throwable;
        }
    }

    private static ArrowBuf zeroedBuffer(BufferAllocator allocator, int size) {
        var buffer = allocator.buffer(Math.max(size, 1));
        buffer.setZero(0, buffer.capacity());
        return buffer;
    }

    private static int validityBufferSize(int rowCount) {
        return (rowCount + 7) >>> 3;
    }

    private static int viewBufferSize(int rowCount) {
        return rowCount * VIEW_WIDTH_BYTES;
    }

    private static void setBit(ArrowBuf buffer, int index) {
        int byteIndex = index >>> 3;
        int mask = 1 << (index & 7);
        buffer.setByte(byteIndex, buffer.getByte(byteIndex) | mask);
    }

    private static void closeAll(List<? extends AutoCloseable> closeables) {
        RuntimeException failure = null;
        for (var closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception exception) {
                if (failure == null) {
                    failure = new RuntimeException("Failed to close partially decoded vectors", exception);
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public static final class DecodedBatch implements AutoCloseable {
        private final VectorSchemaRoot root;

        private DecodedBatch(VectorSchemaRoot root) {
            this.root = root;
        }

        public VectorSchemaRoot root() {
            return root;
        }

        @Override
        public void close() {
            root.close();
        }
    }

    private static final class ParsedLayout {
        private final int rowCount;
        private final List<ColumnLayout> columns;

        private ParsedLayout(int rowCount, List<ColumnLayout> columns) {
            this.rowCount = rowCount;
            this.columns = columns;
        }
    }

    private static final class ColumnLayout {
        private final int[] offsets;
        private final int[] lengths;
        private int nullCount;

        private ColumnLayout(int rowCount) {
            this.offsets = new int[rowCount];
            this.lengths = new int[rowCount];
        }
    }

    private static final class NettyForeignAllocation extends ForeignAllocation {
        private final ByteBuf frame;

        private NettyForeignAllocation(ByteBuf frame) {
            super(frame.capacity(), frame.memoryAddress());
            if (!frame.hasMemoryAddress()) {
                frame.release();
                throw new IllegalArgumentException("Optimized path requires a direct contiguous Netty frame buffer");
            }
            this.frame = frame;
        }

        @Override
        protected void release0() {
            frame.release();
        }
    }
}
