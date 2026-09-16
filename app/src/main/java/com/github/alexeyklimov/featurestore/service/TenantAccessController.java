package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ArrowTenantRequest;
import java.util.concurrent.ConcurrentHashMap;

public final class TenantAccessController {
    private final FeatureCatalog catalog;
    private final ConcurrentHashMap<String, TenantInflightBytes> inflightBytesByTenant = new ConcurrentHashMap<>();

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
        var attempt = new ReservationAttempt();
        inflightBytesByTenant.compute(tenantId, (ignored, inflightBytes) -> {
            var currentInflightBytes = inflightBytes == null ? new TenantInflightBytes() : inflightBytes;
            attempt.granted = currentInflightBytes.tryReserve(bytes, quotaLimit);
            return currentInflightBytes.isEmpty() ? null : currentInflightBytes;
        });
        return attempt.granted;
    }

    private void releaseInflightBytes(String tenantId, long bytes) {
        inflightBytesByTenant.computeIfPresent(tenantId, (ignored, inflightBytes) -> {
            inflightBytes.release(bytes);
            return inflightBytes.isEmpty() ? null : inflightBytes;
        });
    }

    public static final class AuthorizedTenantRequest implements AutoCloseable {
        private final TenantAccessController controller;
        private final String tenantId;
        private final ArrowTenantRequest request;
        private final long maxInflightBytes;
        private final CloseGuard closeGuard = new CloseGuard();

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
            if (!closeGuard.tryClose()) {
                return;
            }
            try {
                request.close();
            } finally {
                controller.releaseInflightBytes(tenantId, request.arrowBytes());
            }
        }
    }

    private static final class ReservationAttempt {
        private boolean granted;
    }

    private static final class TenantInflightBytes {
        private long currentBytes;

        private synchronized boolean tryReserve(long bytes, long quotaLimit) {
            if (bytes > quotaLimit - currentBytes) {
                return false;
            }
            currentBytes += bytes;
            return true;
        }

        private synchronized void release(long bytes) {
            currentBytes = Math.max(0, currentBytes - bytes);
        }

        private synchronized boolean isEmpty() {
            return currentBytes == 0;
        }
    }

    private static final class CloseGuard {
        private boolean closed;

        private synchronized boolean tryClose() {
            if (closed) {
                return false;
            }
            closed = true;
            return true;
        }
    }
}
