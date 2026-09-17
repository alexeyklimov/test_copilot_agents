package com.github.alexeyklimov.featurestore.cassandra;

import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
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
    private static final int ROWS_GLOBAL_TABLES_SPEC_FLAG = 0x0001;
    private static final int VIEW_WIDTH_BYTES = 16;
    private static final int INLINE_BINARY_BYTES = 12;
    private static final ByteBufPrimitiveCodec BYTE_BUF_CODEC = new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT);
    private static final RawType FEATURE_ID_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT);
    private static final RawType BLOB_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB);
    private static final List<Field> RESULT_BATCH_FIELDS = List.of(
            new Field("request_ordinal", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("entity", FieldType.notNullable(ArrowType.Binary.INSTANCE), null),
            new Field("feature_id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
            new Field("value", FieldType.nullable(ArrowType.Binary.INSTANCE), null));

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

    /** Декодирует фиксированный Cassandra-ответ feature_id:int/value:blob в batch основного пайплайна. */
    public DecodedBatch decodeFeatureValueRows(ByteBuf frame, BufferAllocator allocator, long requestOrdinal, byte[] entity) {
        Objects.requireNonNull(frame, "frame must not be null");
        Objects.requireNonNull(allocator, "allocator must not be null");
        Objects.requireNonNull(entity, "entity must not be null");

        var retainedFrame = frame.retainedSlice(frame.readerIndex(), frame.readableBytes());
        try {
            var layout = parseFeatureValueRows(retainedFrame);
            var vectors = new ArrayList<FieldVector>(RESULT_BATCH_FIELDS.size());
            try {
                vectors.add(buildOrdinalVector(allocator, layout.rowCount, requestOrdinal));
                vectors.add(buildEntityVector(allocator, layout.rowCount, entity));
                vectors.add(buildFeatureIdVector(allocator, layout.featureIds));
                vectors.add(buildValueVector(allocator, RESULT_BATCH_FIELDS.get(3), retainedFrame, layout.rowCount, layout.values));
                return new DecodedBatch(new VectorSchemaRoot(RESULT_BATCH_FIELDS, vectors, layout.rowCount));
            } catch (Throwable throwable) {
                closeAll(vectors);
                throw throwable;
            }
        } finally {
            retainedFrame.release();
        }
    }

    private ParsedLayout parse(ByteBuf frame) {
        var resultLayout = parseRowsLayout(frame, columnNames.size(), this::validateBlobColumns);
        int index = resultLayout.rowDataOffset;
        int rowCount = resultLayout.rowCount;
        int columnCount = columnNames.size();

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

    private FeatureValueLayout parseFeatureValueRows(ByteBuf frame) {
        var resultLayout = parseRowsLayout(frame, 2, PreparedBlobRowsArrowDecoder::validateFeatureValueColumns);
        int index = resultLayout.rowDataOffset;
        int rowCount = resultLayout.rowCount;

        var featureIds = new int[rowCount];
        var values = new ColumnLayout(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            require(index + (2 * Integer.BYTES) <= frame.readableBytes(),
                    "Malformed frame: missing feature_id cell for row %s".formatted(rowIndex));
            int featureIdLength = frame.getInt(index);
            index += Integer.BYTES;
            require(featureIdLength == Integer.BYTES,
                    "Malformed frame: feature_id cell must be exactly %s bytes".formatted(Integer.BYTES));
            featureIds[rowIndex] = frame.getInt(index);
            index += Integer.BYTES;

            require(index + Integer.BYTES <= frame.readableBytes(),
                    "Malformed frame: missing value length for row %s".formatted(rowIndex));
            int length = frame.getInt(index);
            index += Integer.BYTES;
            if (length == -1) {
                values.lengths[rowIndex] = -1;
                values.nullCount++;
                continue;
            }
            require(length >= 0, "Malformed frame: negative blob length %s".formatted(length));
            require((long) index + length <= frame.readableBytes(),
                    "Malformed frame: blob payload overruns the retained frame");
            values.offsets[rowIndex] = index;
            values.lengths[rowIndex] = length;
            index += length;
        }
        require(index == frame.readableBytes(), "Malformed frame: trailing bytes remain after feature/value row decode");
        return new FeatureValueLayout(rowCount, featureIds, values);
    }

    private RowsLayout parseRowsLayout(ByteBuf frame, int expectedColumnCount, java.util.function.Consumer<RowsMetadata> metadataValidator) {
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

        var body = frame.duplicate();
        body.readerIndex(index);
        var metadata = RowsMetadata.decode(body, BYTE_BUF_CODEC, false, SUPPORTED_PROTOCOL_VERSION);
        require(metadata.columnCount == expectedColumnCount,
                "Unexpected ROWS column count: expected %s, got %s".formatted(expectedColumnCount, metadata.columnCount));
        if ((metadata.flags & ROWS_NO_METADATA_FLAG) == 0) {
            require(metadata.columnSpecs != null && metadata.columnSpecs.size() == expectedColumnCount,
                    "Optimized path requires column specs when ROWS metadata is present");
            metadataValidator.accept(metadata);
        }

        require(body.readerIndex() + Integer.BYTES <= frame.readableBytes(), "Malformed frame: missing row count");
        int rowCount = body.readInt();
        require(rowCount >= 0, "Malformed frame: negative row count");
        return new RowsLayout(rowCount, body.readerIndex());
    }

    private void validateBlobColumns(RowsMetadata metadata) {
        for (int columnIndex = 0; columnIndex < metadata.columnSpecs.size(); columnIndex++) {
            var columnSpec = metadata.columnSpecs.get(columnIndex);
            require(columnNames.get(columnIndex).equals(columnSpec.name),
                    "Unexpected ROWS column name at index %s: expected %s, got %s"
                            .formatted(columnIndex, columnNames.get(columnIndex), columnSpec.name));
            require(BLOB_TYPE.equals(columnSpec.type),
                    "Unexpected ROWS column type at index %s: expected blob".formatted(columnIndex));
        }
    }

    private static void validateFeatureValueColumns(RowsMetadata metadata) {
        var featureId = metadata.columnSpecs.get(0);
        require("feature_id".equals(featureId.name), "Unexpected first ROWS column name: expected feature_id");
        require(FEATURE_ID_TYPE.equals(featureId.type), "Unexpected feature_id ROWS column type: expected int");

        var value = metadata.columnSpecs.get(1);
        require("value".equals(value.name), "Unexpected second ROWS column name: expected value");
        require(BLOB_TYPE.equals(value.type), "Unexpected value ROWS column type: expected blob");
    }

    private static ViewVarBinaryVector buildVector(
            BufferAllocator allocator,
            Field field,
            ByteBuf retainedFrame,
            int rowCount,
            ColumnLayout column
    ) {
        return buildValueVector(allocator, field, retainedFrame, rowCount, column);
    }

    private static ViewVarBinaryVector buildValueVector(
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

    private static BigIntVector buildOrdinalVector(BufferAllocator allocator, int rowCount, long requestOrdinal) {
        var vector = new BigIntVector("request_ordinal", allocator);
        try {
            vector.allocateNew(rowCount);
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                vector.set(rowIndex, requestOrdinal);
            }
            vector.setValueCount(rowCount);
            return vector;
        } catch (Throwable throwable) {
            vector.close();
            throw throwable;
        }
    }

    private static VarBinaryVector buildEntityVector(BufferAllocator allocator, int rowCount, byte[] entity) {
        var vector = new VarBinaryVector("entity", allocator);
        try {
            vector.allocateNew();
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                vector.setSafe(rowIndex, entity);
            }
            vector.setValueCount(rowCount);
            return vector;
        } catch (Throwable throwable) {
            vector.close();
            throw throwable;
        }
    }

    private static IntVector buildFeatureIdVector(BufferAllocator allocator, int[] featureIds) {
        var vector = new IntVector("feature_id", allocator);
        try {
            vector.allocateNew(featureIds.length);
            for (int rowIndex = 0; rowIndex < featureIds.length; rowIndex++) {
                vector.set(rowIndex, featureIds[rowIndex]);
            }
            vector.setValueCount(featureIds.length);
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

    private static final class RowsLayout {
        private final int rowCount;
        private final int rowDataOffset;

        private RowsLayout(int rowCount, int rowDataOffset) {
            this.rowCount = rowCount;
            this.rowDataOffset = rowDataOffset;
        }
    }

    private static final class FeatureValueLayout {
        private final int rowCount;
        private final int[] featureIds;
        private final ColumnLayout values;

        private FeatureValueLayout(int rowCount, int[] featureIds, ColumnLayout values) {
            this.rowCount = rowCount;
            this.featureIds = featureIds;
            this.values = values;
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
