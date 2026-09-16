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
    private final Map<String, List<BoundFeature>> featureBindingsByName;

    /** Создает кодек для заданного каталога фичей. */
    public LegacyJsonArrowCodec(FeatureCatalog catalog) {
        this.catalog = catalog;
        this.featureBindingsByName = buildFeatureBindingsByName(catalog);
    }

    /** Разбирает legacy JSON в Arrow-запрос арендатора. */
    public ArrowTenantRequest parse(InputStream inputStream, BufferAllocator parentAllocator) throws IOException {
        var requestAllocator = parentAllocator.newChildAllocator("tenant-request", 0, Long.MAX_VALUE);
        var builders = new LinkedHashMap<FeatureCatalog.KeyType, RequestTableBuilder>();
        var requestedFeatures = new LinkedHashSet<String>();
        try (var parser = jsonFactory.createParser(inputStream)) {
            var keyCount = parseRequest(parser, requestAllocator, builders, requestedFeatures);
            validateParsedRequest(keyCount, requestedFeatures);
            var featureIdsByKeyType = resolveFeatureIdsByKeyType(requestedFeatures, builders.keySet());
            var requests = buildRequests(builders, featureIdsByKeyType);
            var totalBytes = totalArrowBytes(requests);
            return new ArrowTenantRequest(requestAllocator, requests, keyCount, keyCount * requestedFeatures.size(), totalBytes);
        } catch (Exception exception) {
            closeAll(builders.values());
            requestAllocator.close();
            if (exception instanceof ReadRequestException readRequestException) {
                throw readRequestException;
            }
            throw exception;
        }
    }

    /** Создает writer для JSON-ответа из Arrow-потока. */
    public JsonArrowResponseWriter newResponseWriter(OutputStream outputStream) throws IOException {
        return new JsonArrowResponseWriter(jsonFactory.createGenerator(outputStream));
    }

    private long parseRequest(
            com.fasterxml.jackson.core.JsonParser parser,
            BufferAllocator requestAllocator,
            Map<FeatureCatalog.KeyType, RequestTableBuilder> builders,
            LinkedHashSet<String> requestedFeatures
    ) throws IOException {
        require(parser.nextToken() == JsonToken.START_OBJECT, "Request payload must be a JSON object");
        long ordinal = 0;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            ordinal = parseTopLevelField(parser, requestAllocator, builders, requestedFeatures, ordinal);
        }
        return ordinal;
    }

    private long parseTopLevelField(
            com.fasterxml.jackson.core.JsonParser parser,
            BufferAllocator requestAllocator,
            Map<FeatureCatalog.KeyType, RequestTableBuilder> builders,
            LinkedHashSet<String> requestedFeatures,
            long ordinal
    ) throws IOException {
        var fieldName = parser.currentName();
        var token = parser.nextToken();
        return switch (fieldName) {
            case "keys" -> parseKeys(parser, token, requestAllocator, builders, ordinal);
            case "features" -> {
                parseFeatures(parser, token, requestedFeatures);
                yield ordinal;
            }
            default -> {
                parser.skipChildren();
                yield ordinal;
            }
        };
    }

    private long parseKeys(
            com.fasterxml.jackson.core.JsonParser parser,
            JsonToken token,
            BufferAllocator requestAllocator,
            Map<FeatureCatalog.KeyType, RequestTableBuilder> builders,
            long ordinal
    ) throws IOException {
        require(token == JsonToken.START_ARRAY, "'keys' must be an array");
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            ordinal = appendKey(parser, requestAllocator, builders, ordinal);
        }
        return ordinal;
    }

    private long appendKey(
            com.fasterxml.jackson.core.JsonParser parser,
            BufferAllocator requestAllocator,
            Map<FeatureCatalog.KeyType, RequestTableBuilder> builders,
            long ordinal
    ) throws IOException {
        require(parser.currentToken() == JsonToken.START_OBJECT, "Each key entry must be an object");
        require(parser.nextToken() == JsonToken.FIELD_NAME, "Each key entry must contain exactly one field");
        var keyName = parser.currentName();
        var keyType = catalog.findKeyType(keyName)
                .orElseThrow(() -> new ReadRequestException(400, "Unknown key type: " + keyName));
        require(parser.nextToken() == JsonToken.VALUE_STRING, "Key values must be strings");
        var entity = parser.getText().getBytes(StandardCharsets.UTF_8);
        require(parser.nextToken() == JsonToken.END_OBJECT, "Each key entry must contain exactly one field");
        builders.computeIfAbsent(keyType, ignored -> new RequestTableBuilder(keyType, requestAllocator))
                .append(ordinal, entity);
        return ordinal + 1;
    }

    private static void parseFeatures(
            com.fasterxml.jackson.core.JsonParser parser,
            JsonToken token,
            LinkedHashSet<String> requestedFeatures
    ) throws IOException {
        require(token == JsonToken.START_ARRAY, "'features' must be an array");
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            require(parser.currentToken() == JsonToken.VALUE_STRING, "Feature names must be strings");
            requestedFeatures.add(parser.getText());
        }
    }

    private static void validateParsedRequest(long keyCount, LinkedHashSet<String> requestedFeatures) {
        if (keyCount == 0) {
            throw new ReadRequestException(400, "Request must contain at least one key");
        }
        if (requestedFeatures.isEmpty()) {
            throw new ReadRequestException(400, "Request must contain at least one feature");
        }
    }

    private Map<FeatureCatalog.KeyType, List<Integer>> resolveFeatureIdsByKeyType(
            LinkedHashSet<String> requestedFeatures,
            java.util.Set<FeatureCatalog.KeyType> requestKeyTypes
    ) {
        var featureIdsByKeyType = new LinkedHashMap<FeatureCatalog.KeyType, List<Integer>>();
        for (var featureName : requestedFeatures) {
            appendFeatureIds(featureIdsByKeyType, featureName, requestKeyTypes);
        }
        return featureIdsByKeyType;
    }

    private void appendFeatureIds(
            Map<FeatureCatalog.KeyType, List<Integer>> featureIdsByKeyType,
            String featureName,
            java.util.Set<FeatureCatalog.KeyType> requestKeyTypes
    ) {
        var matched = false;
        for (var binding : findBoundFeatures(featureName)) {
            if (!requestKeyTypes.contains(binding.keyType())) {
                continue;
            }
            featureIdsByKeyType.computeIfAbsent(binding.keyType(), ignored -> new ArrayList<>())
                    .add(binding.feature().id());
            matched = true;
        }
        if (!matched) {
            throw new ReadRequestException(400, "Feature is not bound to a key type: " + featureName);
        }
    }

    private List<BoundFeature> findBoundFeatures(String featureName) {
        var boundFeatures = featureBindingsByName.get(featureName);
        if (boundFeatures == null) {
            throw new ReadRequestException(400, "Unknown feature: " + featureName);
        }
        return boundFeatures;
    }

    private static Map<String, List<BoundFeature>> buildFeatureBindingsByName(FeatureCatalog catalog) {
        var bindings = new LinkedHashMap<String, List<BoundFeature>>();
        for (var keyType : catalog.keyTypes()) {
            for (var feature : keyType.featuresByName().values()) {
                bindings.computeIfAbsent(feature.name(), ignored -> new ArrayList<>())
                        .add(new BoundFeature(keyType, feature));
            }
        }
        return Map.copyOf(bindings);
    }

    private static List<SliceReadRequest> buildRequests(
            Map<FeatureCatalog.KeyType, RequestTableBuilder> builders,
            Map<FeatureCatalog.KeyType, List<Integer>> featureIdsByKeyType
    ) {
        var requests = new ArrayList<SliceReadRequest>();
        for (var entry : builders.entrySet()) {
            var featureIds = featureIdsByKeyType.getOrDefault(entry.getKey(), List.of())
                    .stream()
                    .mapToInt(Integer::intValue)
                    .toArray();
            requests.add(entry.getValue().build(featureIds));
        }
        return requests;
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

    private record BoundFeature(FeatureCatalog.KeyType keyType, FeatureCatalog.FeatureDefinition feature) {
    }

    public static final class JsonArrowResponseWriter implements AutoCloseable {
        private final JsonGenerator generator;
        private String currentKeyName;
        private String currentEntity;
        private boolean started;

        /** Инициализирует writer и открывает JSON-массив. */
        JsonArrowResponseWriter(JsonGenerator generator) throws IOException {
            this.generator = generator;
            generator.writeStartArray();
            started = true;
        }

        /** Принимает Arrow-батч и сразу пишет JSON-ответ в поток. */
        public void consume(SliceReadRequest request, VectorSchemaRoot batch) throws IOException {
            var entities = (VarBinaryVector) batch.getVector("entity");
            var featureIds = (IntVector) batch.getVector("feature_id");
            var values = (VarBinaryVector) batch.getVector("value");
            for (int row = 0; row < batch.getRowCount(); row++) {
                var entity = new String(entities.get(row), StandardCharsets.UTF_8);
                var keyName = request.keyType().name();
                if (!keyName.equals(currentKeyName) || !entity.equals(currentEntity)) {
                    flushCurrent();
                    currentKeyName = keyName;
                    currentEntity = entity;
                    generator.writeStartObject();
                    generator.writeStringField("key", currentKeyName);
                    generator.writeStringField("key_value", currentEntity);
                    generator.writeObjectFieldStart("features");
                }
                var feature = request.keyType().featureById(featureIds.get(row));
                writeDecodedField(feature.name(), feature.valueEncoding(), values.get(row));
            }
        }

        /** Декодирует бинарное значение фичи и сразу пишет его в JSON. */
        private void writeDecodedField(String fieldName, FeatureCatalog.ValueEncoding valueEncoding, byte[] bytes) throws IOException {
            switch (valueEncoding) {
                case INT32 -> generator.writeNumberField(
                        fieldName,
                        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt());
                case UTF8 -> generator.writeStringField(fieldName, new String(bytes, StandardCharsets.UTF_8));
            }
        }

        /** Завершает текущий объект в JSON-потоке. */
        private void flushCurrent() throws IOException {
            if (currentEntity == null) {
                return;
            }
            generator.writeEndObject();
            generator.writeEndObject();
            currentKeyName = null;
            currentEntity = null;
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
