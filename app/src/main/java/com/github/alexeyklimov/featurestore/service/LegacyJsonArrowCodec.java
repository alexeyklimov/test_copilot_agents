package com.github.alexeyklimov.featurestore.service;

import static com.github.alexeyklimov.featurestore.service.ArrowMessages.totalArrowBytes;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ArrowTenantRequest;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.RequestTableBuilder;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.SliceReadRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public final class LegacyJsonArrowCodec {
    private final JsonFactory jsonFactory = new JsonFactory();
    private final FeatureCatalog catalog;

    /** Создает кодек для заданного каталога фичей. */
    public LegacyJsonArrowCodec(FeatureCatalog catalog) {
        this.catalog = catalog;
    }

    /** Разбирает legacy JSON в Arrow-запрос арендатора. */
    public ArrowTenantRequest parse(InputStream inputStream, BufferAllocator parentAllocator) throws IOException {
        var requestAllocator = parentAllocator.newChildAllocator("tenant-request", 0, Long.MAX_VALUE);
        var builders = new LinkedHashMap<FeatureCatalog.KeyType, RequestTableBuilder>();
        var requestedFeatures = new LinkedHashSet<String>();
        long keyCount = 0;
        long ordinal = 0;
        try (var parser = jsonFactory.createParser(inputStream)) {
            require(parser.nextToken() == JsonToken.START_OBJECT, "Request payload must be a JSON object");
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                var fieldName = parser.currentName();
                var token = parser.nextToken();
                switch (fieldName) {
                    case "keys" -> {
                        require(token == JsonToken.START_ARRAY, "'keys' must be an array");
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            require(parser.currentToken() == JsonToken.START_OBJECT, "Each key entry must be an object");
                            require(parser.nextToken() == JsonToken.FIELD_NAME, "Each key entry must contain exactly one field");
                            var keyName = parser.currentName();
                            var keyType = catalog.findKeyType(keyName)
                                    .orElseThrow(() -> new ReadRequestException(400, "Unknown key type: " + keyName));
                            require(parser.nextToken() == JsonToken.VALUE_STRING, "Key values must be strings");
                            var entity = parser.getText().getBytes(StandardCharsets.UTF_8);
                            require(parser.nextToken() == JsonToken.END_OBJECT, "Each key entry must contain exactly one field");
                            builders.computeIfAbsent(keyType, ignored -> new RequestTableBuilder(keyType, requestAllocator))
                                    .append(ordinal++, entity);
                            keyCount++;
                        }
                    }
                    case "features" -> {
                        require(token == JsonToken.START_ARRAY, "'features' must be an array");
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            require(parser.currentToken() == JsonToken.VALUE_STRING, "Feature names must be strings");
                            requestedFeatures.add(parser.getText());
                        }
                    }
                    default -> parser.skipChildren();
                }
            }
        } catch (Exception exception) {
            closeAll(builders.values());
            requestAllocator.close();
            if (exception instanceof ReadRequestException readRequestException) {
                throw readRequestException;
            }
            throw exception;
        }

        if (keyCount == 0) {
            closeAll(builders.values());
            requestAllocator.close();
            throw new ReadRequestException(400, "Request must contain at least one key");
        }
        if (requestedFeatures.isEmpty()) {
            closeAll(builders.values());
            requestAllocator.close();
            throw new ReadRequestException(400, "Request must contain at least one feature");
        }

        var featureIdsByKeyType = new LinkedHashMap<FeatureCatalog.KeyType, List<Integer>>();
        for (var featureName : requestedFeatures) {
            var feature = catalog.findFeature(featureName)
                    .orElseThrow(() -> new ReadRequestException(400, "Unknown feature: " + featureName));
            var keyType = catalog.keyTypes().stream()
                    .filter(candidate -> candidate.featuresByName().containsKey(featureName))
                    .findFirst()
                    .orElseThrow(() -> new ReadRequestException(400, "Feature is not bound to a key type: " + featureName));
            featureIdsByKeyType.computeIfAbsent(keyType, ignored -> new ArrayList<>()).add(feature.id());
        }

        var requests = new ArrayList<SliceReadRequest>();
        for (var entry : builders.entrySet()) {
            var featureIds = featureIdsByKeyType.getOrDefault(entry.getKey(), List.of())
                    .stream()
                    .mapToInt(Integer::intValue)
                    .toArray();
            requests.add(entry.getValue().build(featureIds));
        }

        var totalBytes = totalArrowBytes(requests);
        return new ArrowTenantRequest(requestAllocator, requests, keyCount, keyCount * requestedFeatures.size(), totalBytes);
    }

    /** Создает writer для JSON-ответа из Arrow-потока. */
    public JsonArrowResponseWriter newResponseWriter(OutputStream outputStream) throws IOException {
        return new JsonArrowResponseWriter(jsonFactory.createGenerator(outputStream));
    }

    /** Проверяет условие и выбрасывает ошибку запроса. */
    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new ReadRequestException(400, message);
        }
    }

    /** Закрывает все промежуточные билдеры запроса. */
    private static void closeAll(Iterable<RequestTableBuilder> builders) {
        for (var builder : builders) {
            builder.close();
        }
    }

    public static final class JsonArrowResponseWriter implements AutoCloseable {
        private final JsonGenerator generator;
        private String currentKeyName;
        private String currentEntity;
        private SliceReadRequest currentRequest;
        private Map<String, Object> currentFeatures;
        private boolean started;

        /** Инициализирует writer и открывает JSON-массив. */
        JsonArrowResponseWriter(JsonGenerator generator) throws IOException {
            this.generator = generator;
            generator.writeStartArray();
            started = true;
        }

        /** Принимает Arrow-батч и накапливает JSON-ответ. */
        public void consume(SliceReadRequest request, VectorSchemaRoot batch) throws IOException {
            var ordinals = (org.apache.arrow.vector.BigIntVector) batch.getVector("request_ordinal");
            var entities = (VarBinaryVector) batch.getVector("entity");
            var featureIds = (IntVector) batch.getVector("feature_id");
            var values = (VarBinaryVector) batch.getVector("value");
            for (int row = 0; row < batch.getRowCount(); row++) {
                var entity = new String(entities.get(row), StandardCharsets.UTF_8);
                var keyName = request.keyType().name();
                if (currentFeatures == null || !keyName.equals(currentKeyName) || !entity.equals(currentEntity)) {
                    flushCurrent();
                    currentKeyName = keyName;
                    currentEntity = entity;
                    currentRequest = request;
                    currentFeatures = new LinkedHashMap<>();
                }
                var feature = request.keyType().featureById(featureIds.get(row));
                currentFeatures.put(feature.name(), decode(feature.valueEncoding(), values.get(row)));
            }
        }

        /** Декодирует бинарное значение фичи. */
        private Object decode(FeatureCatalog.ValueEncoding valueEncoding, byte[] bytes) {
            return switch (valueEncoding) {
                case INT32 -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
                case UTF8 -> new String(bytes, StandardCharsets.UTF_8);
            };
        }

        /** Сбрасывает накопленный объект в JSON-поток. */
        private void flushCurrent() throws IOException {
            if (currentFeatures == null || currentFeatures.isEmpty()) {
                return;
            }
            generator.writeStartObject();
            generator.writeStringField("key", currentKeyName);
            generator.writeStringField("key_value", currentEntity);
            generator.writeObjectFieldStart("features");
            for (var featureId : currentRequest.featureIds()) {
                var feature = currentRequest.keyType().featureById(featureId);
                var value = currentFeatures.get(feature.name());
                if (value != null) {
                    generator.writeObjectField(feature.name(), value);
                }
            }
            generator.writeEndObject();
            generator.writeEndObject();
            currentFeatures = null;
            currentRequest = null;
            generator.flush();
        }

        /** Завершает JSON-массив и закрывает writer. */
        @Override
        public void close() throws IOException {
            if (!started) {
                return;
            }
            flushCurrent();
            generator.writeEndArray();
            generator.close();
            started = false;
        }
    }
}
