package com.github.alexeyklimov.featurestore.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.alexeyklimov.featurestore.model.FeatureCatalog;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

class TenantAccessControllerTest {
    private static final FeatureCatalog.KeyType USER_ID = FeatureCatalog.KeyType.of(
            "user_id",
            1,
            "user_features",
            new FeatureCatalog.FeatureDefinition("feature1", 101, FeatureCatalog.ValueEncoding.INT32));

    @Test
    void enforcesInflightQuotaPerTenantAndReleasesOnClose() {
        var catalog = new FeatureCatalog(
                List.of(USER_ID),
                List.of(new FeatureCatalog.TenantPolicy(
                        "tenant-a",
                        Set.of("user_id"),
                        new FeatureCatalog.RequestQuota(100, 100, 100))));
        var controller = new TenantAccessController(catalog);

        var firstRequest = requestOf(60);
        var secondRequest = requestOf(50);
        TenantAccessController.AuthorizedTenantRequest firstAuthorized = null;
        TenantAccessController.AuthorizedTenantRequest secondAuthorized = null;
        try {
            firstAuthorized = controller.authorize("tenant-a", firstRequest);
            assertThatThrownBy(() -> controller.authorize("tenant-a", secondRequest))
                    .isInstanceOf(ReadRequestException.class)
                    .hasMessageContaining("Tenant quota exceeded");
            firstAuthorized.close();
            firstAuthorized = null;
            secondAuthorized = controller.authorize("tenant-a", secondRequest);
            secondAuthorized.close();
            secondAuthorized = null;
        } finally {
            closeQuietly(firstAuthorized);
            closeQuietly(secondAuthorized);
            closeQuietly(firstRequest);
            closeQuietly(secondRequest);
        }
    }

    @Test
    void countsResponseInflightBytesAgainstTenantQuota() {
        var catalog = new FeatureCatalog(
                List.of(USER_ID),
                List.of(new FeatureCatalog.TenantPolicy(
                        "tenant-a",
                        Set.of("user_id"),
                        new FeatureCatalog.RequestQuota(100, 100, 100))));
        var controller = new TenantAccessController(catalog);

        var request = requestOf(70);
        TenantAccessController.AuthorizedTenantRequest authorized = null;
        try {
            authorized = controller.authorize("tenant-a", request);
            var requestHandle = authorized;
            assertThatThrownBy(() -> controller.reserveResponseInflightBytes(requestHandle, 31))
                    .isInstanceOf(ReadRequestException.class)
                    .hasMessageContaining("Tenant quota exceeded");
            controller.reserveResponseInflightBytes(requestHandle, 30);
            controller.releaseResponseInflightBytes(requestHandle, 30);
            authorized.close();
            authorized = null;
        } finally {
            closeQuietly(authorized);
            closeQuietly(request);
        }
    }

    @Test
    void keepsInflightQuotaIndependentAcrossTenants() {
        var catalog = new FeatureCatalog(
                List.of(USER_ID),
                List.of(
                        new FeatureCatalog.TenantPolicy(
                                "tenant-a",
                                Set.of("user_id"),
                                new FeatureCatalog.RequestQuota(100, 100, 100)),
                        new FeatureCatalog.TenantPolicy(
                                "tenant-b",
                                Set.of("user_id"),
                                new FeatureCatalog.RequestQuota(100, 100, 100))));
        var controller = new TenantAccessController(catalog);

        var tenantARequest = requestOf(90);
        var tenantBRequest = requestOf(90);
        TenantAccessController.AuthorizedTenantRequest authorizedA = null;
        TenantAccessController.AuthorizedTenantRequest authorizedB = null;
        try {
            authorizedA = controller.authorize("tenant-a", tenantARequest);
            authorizedB = controller.authorize("tenant-b", tenantBRequest);
            authorizedA.close();
            authorizedB.close();
            authorizedA = null;
            authorizedB = null;
        } finally {
            closeQuietly(authorizedA);
            closeQuietly(authorizedB);
            closeQuietly(tenantARequest);
            closeQuietly(tenantBRequest);
        }
    }

    @Test
    void closesAuthorizedRequestOnlyOnce() {
        var catalog = new FeatureCatalog(
                List.of(USER_ID),
                List.of(new FeatureCatalog.TenantPolicy(
                        "tenant-a",
                        Set.of("user_id"),
                        new FeatureCatalog.RequestQuota(100, 100, 100))));
        var controller = new TenantAccessController(catalog);

        var firstRequest = requestOf(60);
        var secondRequest = requestOf(40);
        var thirdRequest = requestOf(70);
        TenantAccessController.AuthorizedTenantRequest firstAuthorized = null;
        TenantAccessController.AuthorizedTenantRequest secondAuthorized = null;
        try {
            firstAuthorized = controller.authorize("tenant-a", firstRequest);
            secondAuthorized = controller.authorize("tenant-a", secondRequest);

            firstAuthorized.close();
            firstAuthorized.close();
            firstAuthorized = null;

            assertThatThrownBy(() -> controller.authorize("tenant-a", thirdRequest))
                    .isInstanceOf(ReadRequestException.class)
                    .hasMessageContaining("Tenant quota exceeded");

            secondAuthorized.close();
            secondAuthorized = null;
        } finally {
            closeQuietly(firstAuthorized);
            closeQuietly(secondAuthorized);
            closeQuietly(firstRequest);
            closeQuietly(secondRequest);
            closeQuietly(thirdRequest);
        }
    }

    private static ArrowMessages.ArrowTenantRequest requestOf(long arrowBytes) {
        var allocator = new RootAllocator();
        var root = ArrowMessages.newRequestRoot(allocator);
        root.allocateNew();
        return new ArrowMessages.ArrowTenantRequest(
                allocator,
                List.of(new ArrowMessages.SliceReadRequest(USER_ID, root, new int[]{101})),
                1,
                1,
                arrowBytes);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // no-op for test cleanup
        }
    }
}
