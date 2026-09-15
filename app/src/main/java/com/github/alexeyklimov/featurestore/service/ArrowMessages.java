package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

final class ArrowMessages {
    private static final Schema REQUEST_SCHEMA = new Schema(List.of(
            new Field("request_ordinal", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("entity", FieldType.notNullable(ArrowType.Binary.INSTANCE), null)));
    private static final Schema RESULT_SCHEMA = new Schema(List.of(
            new Field("request_ordinal", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("entity", FieldType.notNullable(ArrowType.Binary.INSTANCE), null),
            new Field("feature_id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
            new Field("value", FieldType.notNullable(ArrowType.Binary.INSTANCE), null)));

    private ArrowMessages() {
    }

    static VectorSchemaRoot newRequestRoot(BufferAllocator allocator) {
        return VectorSchemaRoot.create(REQUEST_SCHEMA, allocator);
    }

    static VectorSchemaRoot newResultRoot(BufferAllocator allocator) {
        return VectorSchemaRoot.create(RESULT_SCHEMA, allocator);
    }

    static final class ArrowTenantRequest implements AutoCloseable {
        private final BufferAllocator allocator;
        private final List<SliceReadRequest> sliceRequests;
        private final long keyCount;
        private final long featureReferenceCount;
        private final long arrowBytes;

        ArrowTenantRequest(
                BufferAllocator allocator,
                List<SliceReadRequest> sliceRequests,
                long keyCount,
                long featureReferenceCount,
                long arrowBytes
        ) {
            this.allocator = allocator;
            this.sliceRequests = List.copyOf(sliceRequests);
            this.keyCount = keyCount;
            this.featureReferenceCount = featureReferenceCount;
            this.arrowBytes = arrowBytes;
        }

        List<SliceReadRequest> sliceRequests() {
            return sliceRequests;
        }

        long keyCount() {
            return keyCount;
        }

        long featureReferenceCount() {
            return featureReferenceCount;
        }

        long arrowBytes() {
            return arrowBytes;
        }

        @Override
        public void close() {
            for (var request : sliceRequests) {
                request.close();
            }
            allocator.close();
        }
    }

    static final class SliceReadRequest implements AutoCloseable {
        private final FeatureCatalog.KeyType keyType;
        private final VectorSchemaRoot root;
        private final int[] featureIds;

        SliceReadRequest(FeatureCatalog.KeyType keyType, VectorSchemaRoot root, int[] featureIds) {
            this.keyType = keyType;
            this.root = root;
            this.featureIds = featureIds;
        }

        FeatureCatalog.KeyType keyType() {
            return keyType;
        }

        VectorSchemaRoot root() {
            return root;
        }

        int[] featureIds() {
            return featureIds;
        }

        int rowCount() {
            return root.getRowCount();
        }

        long requestOrdinal(int index) {
            return ordinalVector().get(index);
        }

        byte[] entity(int index) {
            return entityVector().get(index);
        }

        private BigIntVector ordinalVector() {
            return (BigIntVector) root.getVector("request_ordinal");
        }

        private VarBinaryVector entityVector() {
            return (VarBinaryVector) root.getVector("entity");
        }

        @Override
        public void close() {
            root.close();
        }
    }

    static final class RequestTableBuilder implements AutoCloseable {
        private final FeatureCatalog.KeyType keyType;
        private final VectorSchemaRoot root;
        private final BigIntVector ordinals;
        private final VarBinaryVector entities;
        private int rowCount;

        RequestTableBuilder(FeatureCatalog.KeyType keyType, BufferAllocator allocator) {
            this.keyType = keyType;
            this.root = newRequestRoot(allocator);
            this.ordinals = (BigIntVector) root.getVector("request_ordinal");
            this.entities = (VarBinaryVector) root.getVector("entity");
            root.allocateNew();
        }

        void append(long ordinal, byte[] entity) {
            ordinals.setSafe(rowCount, ordinal);
            entities.setSafe(rowCount, entity);
            rowCount++;
        }

        long bufferSize() {
            root.setRowCount(rowCount);
            return root.getFieldVectors().stream().mapToLong(vector -> vector.getBufferSize()).sum();
        }

        SliceReadRequest build(int[] featureIds) {
            root.setRowCount(rowCount);
            return new SliceReadRequest(keyType, root, featureIds);
        }

        @Override
        public void close() {
            root.close();
        }
    }

    @FunctionalInterface
    interface ResultBatchConsumer {
        void accept(SliceReadRequest request, VectorSchemaRoot batch) throws Exception;
    }

    static final class ResultTableStreamer implements AutoCloseable {
        private final BufferAllocator allocator;
        private final VectorSchemaRoot root;
        private final BigIntVector ordinals;
        private final VarBinaryVector entities;
        private final IntVector featureIds;
        private final VarBinaryVector values;
        private final int batchSize;
        private int rowCount;

        ResultTableStreamer(BufferAllocator allocator, int batchSize) {
            this.allocator = allocator;
            this.root = newResultRoot(allocator);
            this.ordinals = (BigIntVector) root.getVector("request_ordinal");
            this.entities = (VarBinaryVector) root.getVector("entity");
            this.featureIds = (IntVector) root.getVector("feature_id");
            this.values = (VarBinaryVector) root.getVector("value");
            this.batchSize = batchSize;
            root.allocateNew();
        }

        void append(long ordinal, byte[] entity, int featureId, byte[] value, SliceReadRequest request, ResultBatchConsumer consumer)
                throws Exception {
            ordinals.setSafe(rowCount, ordinal);
            entities.setSafe(rowCount, entity);
            featureIds.setSafe(rowCount, featureId);
            values.setSafe(rowCount, value);
            rowCount++;
            if (rowCount >= batchSize) {
                flush(request, consumer);
            }
        }

        void flush(SliceReadRequest request, ResultBatchConsumer consumer) throws Exception {
            if (rowCount == 0) {
                return;
            }
            root.setRowCount(rowCount);
            consumer.accept(request, root);
            clear();
        }

        private void clear() {
            rowCount = 0;
            root.clear();
            root.allocateNew();
        }

        @Override
        public void close() {
            root.close();
            allocator.close();
        }
    }

    static long totalArrowBytes(List<SliceReadRequest> requests) {
        var size = 0L;
        for (var request : requests) {
            size += request.root().getFieldVectors().stream().mapToLong(vector -> vector.getBufferSize()).sum();
            size += (long) request.featureIds().length * Integer.BYTES;
        }
        return size;
    }
}
