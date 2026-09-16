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

public final class ArrowMessages {
    private static final Schema REQUEST_SCHEMA = new Schema(List.of(
            new Field("request_ordinal", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("entity", FieldType.notNullable(ArrowType.Binary.INSTANCE), null)));
    private static final Schema RESULT_SCHEMA = new Schema(List.of(
            new Field("request_ordinal", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("entity", FieldType.notNullable(ArrowType.Binary.INSTANCE), null),
            new Field("feature_id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
            new Field("value", FieldType.notNullable(ArrowType.Binary.INSTANCE), null)));

    /** Скрывает создание утилитарного класса. */
    private ArrowMessages() {
    }

    /** Создает Arrow-таблицу для входных ключей. */
    static VectorSchemaRoot newRequestRoot(BufferAllocator allocator) {
        return VectorSchemaRoot.create(REQUEST_SCHEMA, allocator);
    }

    /** Создает Arrow-таблицу для результатов чтения. */
    static VectorSchemaRoot newResultRoot(BufferAllocator allocator) {
        return VectorSchemaRoot.create(RESULT_SCHEMA, allocator);
    }

    public static final class ArrowTenantRequest implements AutoCloseable {
        private final BufferAllocator allocator;
        private final List<SliceReadRequest> sliceRequests;
        private final long keyCount;
        private final long featureReferenceCount;
        private final long arrowBytes;

        /** Создает контейнер запроса арендатора. */
        public ArrowTenantRequest(
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

        /** Возвращает список запросов по срезам. */
        public List<SliceReadRequest> sliceRequests() {
            return sliceRequests;
        }

        /** Возвращает число ключей в запросе. */
        public long keyCount() {
            return keyCount;
        }

        /** Возвращает число ссылок на фичи. */
        public long featureReferenceCount() {
            return featureReferenceCount;
        }

        /** Возвращает объем Arrow-данных в байтах. */
        public long arrowBytes() {
            return arrowBytes;
        }

        /** Освобождает все Arrow-ресурсы запроса. */
        @Override
        public void close() {
            for (var request : sliceRequests) {
                request.close();
            }
            allocator.close();
        }
    }

    public static final class SliceReadRequest implements AutoCloseable {
        private final FeatureCatalog.KeyType keyType;
        private final VectorSchemaRoot root;
        private final int[] featureIds;

        /** Создает запрос чтения по конкретному срезу. */
        public SliceReadRequest(FeatureCatalog.KeyType keyType, VectorSchemaRoot root, int[] featureIds) {
            this.keyType = keyType;
            this.root = root;
            this.featureIds = featureIds;
        }

        /** Возвращает тип ключа для текущего среза. */
        public FeatureCatalog.KeyType keyType() {
            return keyType;
        }

        /** Возвращает Arrow-таблицу исходного запроса. */
        public VectorSchemaRoot root() {
            return root;
        }

        /** Возвращает идентификаторы запрошенных фичей. */
        public int[] featureIds() {
            return featureIds;
        }

        /** Возвращает число строк в срезе. */
        public int rowCount() {
            return root.getRowCount();
        }

        /** Возвращает порядковый номер исходного ключа. */
        public long requestOrdinal(int index) {
            return ordinalVector().get(index);
        }

        /** Возвращает бинарное значение сущности. */
        public byte[] entity(int index) {
            return entityVector().get(index);
        }

        /** Возвращает вектор порядковых номеров. */
        private BigIntVector ordinalVector() {
            return (BigIntVector) root.getVector("request_ordinal");
        }

        /** Возвращает вектор сущностей. */
        private VarBinaryVector entityVector() {
            return (VarBinaryVector) root.getVector("entity");
        }

        /** Освобождает ресурсы среза. */
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

        /** Создает билдер Arrow-таблицы для ключей. */
        RequestTableBuilder(FeatureCatalog.KeyType keyType, BufferAllocator allocator) {
            this.keyType = keyType;
            this.root = newRequestRoot(allocator);
            this.ordinals = (BigIntVector) root.getVector("request_ordinal");
            this.entities = (VarBinaryVector) root.getVector("entity");
            root.allocateNew();
        }

        /** Добавляет ключ в Arrow-таблицу запроса. */
        void append(long ordinal, byte[] entity) {
            ordinals.setSafe(rowCount, ordinal);
            entities.setSafe(rowCount, entity);
            rowCount++;
        }

        /** Возвращает текущий размер буферов таблицы. */
        long bufferSize() {
            root.setRowCount(rowCount);
            return root.getFieldVectors().stream().mapToLong(vector -> vector.getBufferSize()).sum();
        }

        /** Собирает итоговый запрос чтения по срезу. */
        SliceReadRequest build(int[] featureIds) {
            root.setRowCount(rowCount);
            return new SliceReadRequest(keyType, root, featureIds);
        }

        /** Освобождает ресурсы билдера. */
        @Override
        public void close() {
            root.close();
        }
    }

    @FunctionalInterface
    public interface ResultBatchConsumer {
        /** Принимает очередной батч результата. */
        void accept(SliceReadRequest request, VectorSchemaRoot batch) throws Exception;
    }

    public static final class ResultTableStreamer implements AutoCloseable {
        private final BufferAllocator allocator;
        private final VectorSchemaRoot root;
        private final BigIntVector ordinals;
        private final VarBinaryVector entities;
        private final IntVector featureIds;
        private final VarBinaryVector values;
        private final int batchSize;
        private final int pageSizeBytes;
        private int rowCount;
        private long bufferedBytes;

        /** Создает стример батчей результата. */
        public ResultTableStreamer(BufferAllocator allocator, int batchSize, int pageSizeBytes) {
            this.allocator = allocator;
            this.root = newResultRoot(allocator);
            this.ordinals = (BigIntVector) root.getVector("request_ordinal");
            this.entities = (VarBinaryVector) root.getVector("entity");
            this.featureIds = (IntVector) root.getVector("feature_id");
            this.values = (VarBinaryVector) root.getVector("value");
            this.batchSize = batchSize;
            this.pageSizeBytes = pageSizeBytes;
            root.allocateNew();
        }

        /** Добавляет строку результата и сбрасывает батч при необходимости. */
        public void append(long ordinal, byte[] entity, int featureId, byte[] value, SliceReadRequest request, ResultBatchConsumer consumer)
                throws Exception {
            long rowBytes = rowSizeBytes(entity, value);
            if (rowBytes > pageSizeBytes) {
                throw new IllegalStateException("Cassandra row exceeds configured page size bytes: " + rowBytes + " > " + pageSizeBytes);
            }
            if (rowCount > 0 && bufferedBytes + rowBytes > pageSizeBytes) {
                flush(request, consumer);
            }
            ordinals.setSafe(rowCount, ordinal);
            entities.setSafe(rowCount, entity);
            featureIds.setSafe(rowCount, featureId);
            values.setSafe(rowCount, value);
            rowCount++;
            bufferedBytes += rowBytes;
            if (rowCount >= batchSize || bufferedBytes >= pageSizeBytes) {
                flush(request, consumer);
            }
        }

        /** Отправляет накопленный батч потребителю. */
        public void flush(SliceReadRequest request, ResultBatchConsumer consumer) throws Exception {
            if (rowCount == 0) {
                return;
            }
            root.setRowCount(rowCount);
            consumer.accept(request, root);
            clear();
        }

        /** Очищает буферы для следующего батча. */
        private void clear() {
            rowCount = 0;
            bufferedBytes = 0;
            root.clear();
            root.allocateNew();
        }

        private static long rowSizeBytes(byte[] entity, byte[] value) {
            return Long.BYTES
                    + Integer.BYTES
                    + (2L * Integer.BYTES)
                    + entity.length
                    + value.length;
        }

        /** Освобождает ресурсы стримера результатов. */
        @Override
        public void close() {
            root.close();
            allocator.close();
        }
    }

    /** Считает общий размер Arrow-представления запроса. */
    static long totalArrowBytes(List<SliceReadRequest> requests) {
        var size = 0L;
        for (var request : requests) {
            size += request.root().getFieldVectors().stream().mapToLong(vector -> vector.getBufferSize()).sum();
            size += (long) request.featureIds().length * Integer.BYTES;
        }
        return size;
    }
}
