package com.github.alexeyklimov.featurestore.service;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import com.github.alexeyklimov.featurestore.service.ArrowMessages.ArrowTenantRequest;

public final class TenantAccessController {
    private final FeatureCatalog catalog;

    /** Создает контроллер доступа по каталогу. */
    public TenantAccessController(FeatureCatalog catalog) {
        this.catalog = catalog;
    }

    /** Проверяет права и квоты арендатора. */
    public ArrowTenantRequest authorize(String tenantId, ArrowTenantRequest request) {
        var policy = catalog.findTenantPolicy(tenantId)
                .orElseThrow(() -> new ReadRequestException(403, "Unknown tenant: " + tenantId));

        if (request.keyCount() > policy.quota().maxKeys()
                || request.featureReferenceCount() > policy.quota().maxFeatureRefs()
                || request.arrowBytes() > policy.quota().maxArrowBytes()) {
            throw new ReadRequestException(429, "Tenant quota exceeded");
        }

        for (var sliceRequest : request.sliceRequests()) {
            if (!policy.allowedKeyTypes().contains(sliceRequest.keyType().name())) {
                throw new ReadRequestException(403, "Tenant cannot access key type: " + sliceRequest.keyType().name());
            }
        }
        return request;
    }
}
