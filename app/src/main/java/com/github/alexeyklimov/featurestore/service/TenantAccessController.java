package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ArrowTenantRequest;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class TenantAccessController {
    private final FeatureCatalog catalog;
    private final ConcurrentHashMap<String, AtomicLong> inflightBytesByTenant = new ConcurrentHashMap<>();

    /** Создает контроллер доступа по каталогу. */
    public TenantAccessController(FeatureCatalog catalog) {
        this.catalog = catalog;
    }

    /** Проверяет арендатора, доступные ключи и квоты. */
    public AuthorizedTenantRequest authorize(String tenantId, ArrowTenantRequest request) {
        var policy = catalog.findTenantPolicy(tenantId)
                .orElseThrow(() -> new ReadRequestException(403, "Unknown tenant: " + tenantId));

        if (request.keyCount() > policy.quota().maxKeys()
                || request.featureReferenceCount() > policy.quota().maxFeatureRefs()
                || request.arrowBytes() > policy.quota().maxArrowBytes()
                || !reserveInflightBytes(tenantId, request.arrowBytes(), policy.quota().maxArrowBytes())) {
            throw new ReadRequestException(429, "Tenant quota exceeded");
        }

        for (var sliceRequest : request.sliceRequests()) {
            if (!policy.allowedKeyTypes().contains(sliceRequest.keyType().name())) {
                releaseInflightBytes(tenantId, request.arrowBytes());
                throw new ReadRequestException(403, "Tenant cannot access key type: " + sliceRequest.keyType().name());
            }
        }
        return new AuthorizedTenantRequest(this, tenantId, request, policy.quota().maxArrowBytes());
    }

    /** Резервирует объем in-flight данных ответа и проверяет квоту арендатора. */
    public void reserveResponseInflightBytes(AuthorizedTenantRequest authorizedRequest, long responseBytes) {
        if (responseBytes <= 0) {
            return;
        }
        if (!reserveInflightBytes(authorizedRequest.tenantId, responseBytes, authorizedRequest.maxInflightBytes)) {
            throw new ReadRequestException(429, "Tenant quota exceeded");
        }
    }

    /** Освобождает объем in-flight данных ответа арендатора. */
    public void releaseResponseInflightBytes(AuthorizedTenantRequest authorizedRequest, long responseBytes) {
        if (responseBytes <= 0) {
            return;
        }
        releaseInflightBytes(authorizedRequest.tenantId, responseBytes);
    }

    private boolean reserveInflightBytes(String tenantId, long bytes, long quotaLimit) {
        var inflightBytes = inflightBytesByTenant.computeIfAbsent(tenantId, ignored -> new AtomicLong());
        while (true) {
            var current = inflightBytes.get();
            if (bytes > quotaLimit - current) {
                return false;
            }
            var updated = current + bytes;
            if (inflightBytes.compareAndSet(current, updated)) {
                return true;
            }
        }
    }

    private void releaseInflightBytes(String tenantId, long bytes) {
        var inflightBytes = inflightBytesByTenant.get(tenantId);
        if (inflightBytes == null) {
            return;
        }
        while (true) {
            var current = inflightBytes.get();
            var updated = Math.max(0, current - bytes);
            if (!inflightBytes.compareAndSet(current, updated)) {
                continue;
            }
            if (updated == 0) {
                inflightBytesByTenant.remove(tenantId, inflightBytes);
            }
            return;
        }
    }

    public static final class AuthorizedTenantRequest implements AutoCloseable {
        private final TenantAccessController controller;
        private final String tenantId;
        private final ArrowTenantRequest request;
        private final long maxInflightBytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private AuthorizedTenantRequest(
                TenantAccessController controller,
                String tenantId,
                ArrowTenantRequest request,
                long maxInflightBytes
        ) {
            this.controller = controller;
            this.tenantId = tenantId;
            this.request = request;
            this.maxInflightBytes = maxInflightBytes;
        }

        public ArrowTenantRequest request() {
            return request;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                request.close();
            } finally {
                controller.releaseInflightBytes(tenantId, request.arrowBytes());
            }
        }
    }
}
