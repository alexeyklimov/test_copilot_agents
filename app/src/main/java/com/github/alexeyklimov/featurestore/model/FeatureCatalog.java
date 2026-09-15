package com.github.alexeyklimov.featurestore.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class FeatureCatalog {
    private final Map<String, KeyType> keyTypesByName;
    private final Map<String, FeatureDefinition> featuresByName;
    private final Map<String, TenantPolicy> tenantPolicies;

    public FeatureCatalog(List<KeyType> keyTypes, List<TenantPolicy> tenantPolicies) {
        this.keyTypesByName = new LinkedHashMap<>();
        this.featuresByName = new LinkedHashMap<>();
        for (var keyType : keyTypes) {
            keyTypesByName.put(keyType.name(), keyType);
            for (var feature : keyType.featuresByName().values()) {
                featuresByName.put(feature.name(), feature);
            }
        }
        this.tenantPolicies = new LinkedHashMap<>();
        for (var policy : tenantPolicies) {
            this.tenantPolicies.put(policy.tenantId(), policy);
        }
    }

    public Optional<KeyType> findKeyType(String name) {
        return Optional.ofNullable(keyTypesByName.get(name));
    }

    public Optional<FeatureDefinition> findFeature(String name) {
        return Optional.ofNullable(featuresByName.get(name));
    }

    public Optional<TenantPolicy> findTenantPolicy(String tenantId) {
        return Optional.ofNullable(tenantPolicies.get(tenantId));
    }

    public Collection<KeyType> keyTypes() {
        return keyTypesByName.values();
    }

    public record KeyType(
            String name,
            int keyId,
            String slice,
            Map<String, FeatureDefinition> featuresByName,
            Map<Integer, FeatureDefinition> featuresById
    ) {
        public static KeyType of(String name, int keyId, String slice, FeatureDefinition... features) {
            var byName = new LinkedHashMap<String, FeatureDefinition>();
            var byId = new LinkedHashMap<Integer, FeatureDefinition>();
            for (var feature : features) {
                byName.put(feature.name(), feature);
                byId.put(feature.id(), feature);
            }
            return new KeyType(name, keyId, slice, Map.copyOf(byName), Map.copyOf(byId));
        }

        public FeatureDefinition featureById(int featureId) {
            return featuresById.get(featureId);
        }
    }

    public record FeatureDefinition(String name, int id, ValueEncoding valueEncoding) {
    }

    public record TenantPolicy(String tenantId, Set<String> allowedKeyTypes, RequestQuota quota) {
    }

    public record RequestQuota(int maxKeys, int maxFeatureRefs, long maxArrowBytes) {
    }

    public enum ValueEncoding {
        INT32,
        UTF8
    }
}
