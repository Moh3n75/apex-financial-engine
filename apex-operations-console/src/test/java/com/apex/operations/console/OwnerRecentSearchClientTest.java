package com.apex.operations.console;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.apex.operations.contracts.OwnerRecentSearch.*;
import com.apex.platform.grpc.api.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

class OwnerRecentSearchClientTest {
    Query query(){return new Query("test","order-cell",Kind.WORKFLOW,"order",Instant.now().minusSeconds(60),Instant.now(),"ANY",16,"");}
    OwnerRecentSearchClient client(QueryGateway gateway) {
        var beans=new DefaultListableBeanFactory();beans.registerSingleton("gateway",gateway);
        return new OwnerRecentSearchClient(new MockEnvironment().withProperty("apex.ops03d.environment","test").withProperty("apex.ops03d.sources","order-cell/WORKFLOW/order"),beans.getBeanProvider(QueryGateway.class));
    }
    Page page(Query q,String owner,Instant asOf) {
        UUID id=UUID.randomUUID();return new Page(1,"test",owner,"one",UUID.randomUUID(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),asOf,asOf,asOf,10000,1,
                List.of(Gap.PARTIAL_CURRENT_STATE,Gap.RETENTION_UNQUALIFIED,Gap.RECONCILE_FROM_START),
                List.of(new Item(id,"CONFIRMED",q.fromInclusive(),q.kind()+":"+id,Evidence.OWNER_COMMITTED_ROW,Certainty.VERIFIED_WITHIN_SCOPE)),"","RELATION_UNKNOWN");
    }
    @Test void unavailableSourceRetainsOnlyExactScopedLastPageWithExplicitStaleness() {
        var gateway=mock(QueryGateway.class);var q=query();var p=page(q,"order-cell",Instant.now());
        when(gateway.query(eq("order-cell"),eq(q),eq(Page.class),any())).thenReturn(p).thenThrow(new GatewayException(GatewayException.Code.UNAVAILABLE,true));
        var c=client(gateway);var first=c.read(q);var failed=c.read(q);
        assertThat(first.freshness()).isEqualTo("FRESH");assertThat(failed.page()).isEqualTo(p);assertThat(failed.freshness()).isEqualTo("STALE");assertThat(failed.availability()).isEqualTo("SOURCE_UNAVAILABLE");
        var changed=new Query(q.environment(),q.owner(),q.kind(),q.scope(),q.fromInclusive().plusSeconds(1),q.toExclusive(),q.state(),16,"");
        assertThat(c.read(changed).page()).isNull();
    }
    @Test void changedAuthorizationDoesNotExposeCachedPage() {
        var gateway=mock(QueryGateway.class);var q=query();
        when(gateway.query(anyString(),eq(q),eq(Page.class),any())).thenReturn(page(q,"order-cell",Instant.now())).thenThrow(new GatewayException(GatewayException.Code.PERMISSION_DENIED,false)).thenThrow(new GatewayException(GatewayException.Code.UNAVAILABLE,true));
        var c=client(gateway);assertThat(c.read(q).page()).isNotNull();var denied=c.read(q);assertThat(denied.page()).isNull();assertThat(denied.availability()).isEqualTo("PERMISSION_GAP");assertThat(c.read(q).page()).isNull();
    }
    @Test void environmentScopeAndIndependentReceiptGrantAreRequiredBeforeCacheOrRpc() {
        var gateway=mock(QueryGateway.class);var c=client(gateway);var q=query();
        for(var changed:List.of(new Query("other",q.owner(),q.kind(),q.scope(),q.fromInclusive(),q.toExclusive(),"ANY",16,""),
                new Query(q.environment(),q.owner(),Kind.REQUEST_RECEIPT,"order-request",q.fromInclusive(),q.toExclusive(),"ANY",16,""),
                new Query(q.environment(),"payment-cell",q.kind(),"authorization-simulation",q.fromInclusive(),q.toExclusive(),"ANY",16,"")))
            assertThatThrownBy(()->c.read(changed)).isInstanceOfSatisfying(ResponseStatusException.class,e->assertThat(e.getStatusCode().value()).isEqualTo(403));
        verifyNoInteractions(gateway);
    }
    @Test void wrongSourceIsQuarantinedAndFutureObservationIsClockUncertain() {
        var q=query();var bad=mock(QueryGateway.class);when(bad.query(anyString(),any(),eq(Page.class),any())).thenReturn(page(q,"other",Instant.now()));
        assertThat(client(bad).read(q).page()).isNull();
        var skewed=mock(QueryGateway.class);when(skewed.query(anyString(),any(),eq(Page.class),any())).thenReturn(page(q,"order-cell",Instant.now().plusSeconds(3)));
        assertThat(client(skewed).read(q).freshness()).isEqualTo("CLOCK_UNCERTAIN");
    }
    @Test void actualJavaFirstCodecAndCanonicalWireCapsAreCompatible() {
        var beans=new DefaultListableBeanFactory();beans.registerSingleton("gateway",mock(QueryGateway.class));
        var oversized=new MockEnvironment().withProperty("apex.grpc.max-payload-bytes","65537").withProperty("apex.ops03d.environment","test").withProperty("apex.ops03d.sources","order-cell/WORKFLOW/order");
        assertThatThrownBy(()->new OwnerRecentSearchClient(oversized,beans.getBeanProvider(QueryGateway.class))).isInstanceOf(IllegalStateException.class).hasMessage("OPS_SEARCH_WIRE_POLICY");
        var q=query();assertThat(com.apex.platform.grpc.internal.Contracts.inspect(Query.class,true).type()).isEqualTo("OPS_OWNER_RECENT_SEARCH");
        var requestCodec=new com.apex.platform.grpc.internal.RpcCodec(1024);
        assertThat(requestCodec.decode(requestCodec.encode(q,Query.class),Query.class)).isEqualTo(q);
        var p=page(q,"order-cell",Instant.now());var responseCodec=new com.apex.platform.grpc.internal.RpcCodec(16384);
        assertThat(responseCodec.decode(responseCodec.encode(p,Page.class),Page.class)).isEqualTo(p);
        assertThatThrownBy(()->requestCodec.decode(new byte[1025],Query.class)).isInstanceOf(GatewayException.class);
        assertThat(JsonMapper.builder().build().writeValueAsString(p)).doesNotContain("payload","idempotency","principal","Authorization");
    }
    @Test void listParametersRejectUnknownDuplicatesGiantLimitsAndNonUtcBeforeRpc() {
        var c=mock(OwnerRecentSearchClient.class);
        @SuppressWarnings("unchecked")
        var auth=mock(org.springframework.beans.factory.ObjectProvider.class);
        when(auth.getIfAvailable()).thenReturn(null);
        var api=new RecentSearchApi(c,auth);var p=new org.springframework.util.LinkedMultiValueMap<String,String>();
        p.add("environment","test");p.add("owner","order-cell");p.add("scope","order");p.add("from",Instant.now().minusSeconds(60).toString());p.add("to",Instant.now().toString());
        p.add("sql","CANARY_SECRET");assertThatThrownBy(()->api.workflows(p)).isInstanceOf(ResponseStatusException.class);p.remove("sql");
        p.add("limit","1000000");assertThatThrownBy(()->api.workflows(p)).isInstanceOf(ResponseStatusException.class);p.remove("limit");
        p.add("owner","payment-cell");assertThatThrownBy(()->api.workflows(p)).isInstanceOf(ResponseStatusException.class);p.set("owner","order-cell");
        p.set("from","2026-11-02T10:00:00+01:00");assertThatThrownBy(()->api.workflows(p)).isInstanceOf(ResponseStatusException.class);
        verifyNoInteractions(c);
    }
    @Test void olderAndSameTimeContradictionsAreRetainedWithoutInventedRevision() {
        var gateway=mock(QueryGateway.class);var q=query();var now=Instant.now();var current=page(q,"order-cell",now);
        var older=page(q,"order-cell",now.minusSeconds(1));var sameTime=page(q,"order-cell",now);
        when(gateway.query(anyString(),eq(q),eq(Page.class),any())).thenReturn(current,older,sameTime,current);
        var c=client(gateway);assertThat(c.read(q).page()).isEqualTo(current);
        var outOfOrder=c.read(q);assertThat(outOfOrder.page()).isEqualTo(current);assertThat(outOfOrder.uncertainty()).isEqualTo("CONFLICTING");
        var conflict=c.read(q);assertThat(conflict.priorConflicts()).containsExactly(older,sameTime);assertThat(conflict.page()).isEqualTo(current);
        assertThat(c.read(q).uncertainty()).isEqualTo("CONFLICTING");
    }
    @Test void duplicateProjectionIdentityAndOversizedResponseAreRejected() {
        var q=query();var p=page(q,"order-cell",Instant.now());var item=p.items().getFirst();
        assertThatThrownBy(()->new Page(p.schemaVersion(),p.environment(),p.owner(),p.instance(),p.epoch(),p.kind(),p.scope(),p.fromInclusive(),p.toExclusive(),p.scanStartedAt(),p.scanCompletedAt(),p.asOf(),p.maxAgeMillis(),2,p.coverage(),List.of(item,item),"","RELATION_UNKNOWN")).isInstanceOf(IllegalArgumentException.class);
        var codec=new com.apex.platform.grpc.internal.RpcCodec(16384);
        assertThatThrownBy(()->codec.decode(new byte[16385],Page.class)).isInstanceOf(GatewayException.class);
    }
}
