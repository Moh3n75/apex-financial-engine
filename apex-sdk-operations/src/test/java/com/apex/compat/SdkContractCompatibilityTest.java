package com.apex.compat;

import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import com.apex.platform.events.*;
import com.apex.platform.events.api.*;
import com.apex.platform.grpc.api.*;
import com.apex.platform.grpc.internal.*;
import com.apex.platform.grpc.internal.wire.*;
import com.apex.operations.contracts.*;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.*;

class SdkContractCompatibilityTest {
    @CellRequest(type="SDK_CHANGE",version=1,response=ResponseV1.class) public record ChangeV1(String id) {}
    @CellQuery(type="SDK_READ",version=1,response=ResponseV1.class) public record ReadV1(String id) {}
    @CellQuery(type="SDK_READ",version=2,response=ResponseV2.class) public record ReadV2(String id) {}
    public record ResponseV1(String id) {}
    public record ResponseV2(String id,String currency) { public ResponseV2 {Objects.requireNonNull(currency);} }
    public record PayloadV1(String id) implements IntegrationEvent {}
    public record PayloadV2(String id,String currency) implements IntegrationEvent {}
    final JsonMapper mapper=JsonMapper.builder().build();
    final RpcCodec rpc=new RpcCodec(65536);
    static byte[] bytes(String name) throws Exception {
        try(var input=SdkContractCompatibilityTest.class.getResourceAsStream("/sdk-1/v1/"+name)) {
            if(input==null)throw new IllegalStateException("Missing frozen fixture: "+name);
            return input.readAllBytes();
        }
    }
    @Test void eventEnvelopeAndMetadataKeepTheVersionedWireShape() throws Exception {
        var tree=mapper.readTree(bytes("event-envelope.json"));
        var metadata=mapper.treeToValue(tree.get("metadata"),EventMetadata.class);
        var payload=mapper.treeToValue(tree.get("payload"),PayloadV1.class);
        var envelope=new EventEnvelope<>(metadata,payload);
        assertThat(metadata.eventType()).isEqualTo("SDK_CHANGED");assertThat(metadata.eventVersion()).isEqualTo(1);
        assertThat(metadata.occurredAt()).isEqualTo(Instant.parse("2026-01-02T03:04:05Z"));
        var encoded=mapper.readTree(mapper.writeValueAsBytes(envelope));
        assertThat(mapper.treeToValue(encoded.get("metadata"),EventMetadata.class)).isEqualTo(metadata);
        assertThat(encoded.get("metadata").size()).isEqualTo(10);
        assertThat(encoded.get("metadata").has("schemaId")).isFalse();
        var contract=EventContracts.define("SDK_CHANGED",1,"SDK",PayloadV1.class,"sdk.changed.v1");
        assertThat(contract.schemaId()).isEqualTo("sdk.changed.v1");
        var withDifferentSchema=EventContracts.define("SDK_CHANGED",1,"SDK",PayloadV1.class,"sdk.changed.alternate");
        assertThat(withDifferentSchema.eventType()).isEqualTo(contract.eventType());
        assertThat(withDifferentSchema.eventVersion()).isEqualTo(contract.eventVersion());
        // schemaId describes the Java declaration; it is not a second serialized routing key.
        assertThat(EventMetadata.class.getRecordComponents()).extracting(java.lang.reflect.RecordComponent::getName)
                .doesNotContain("schemaId");
        assertThatThrownBy(() -> EventContracts.define("SDK_CHANGED",0,"SDK",PayloadV1.class,"v0"))
                .isInstanceOf(IllegalArgumentException.class);
    }
    @Test void javaModelsAndStrictResponseEvolutionRequireAnExplicitVersion() throws Exception {
        assertThat(rpc.decode(bytes("rpc-request.json"),ChangeV1.class)).isEqualTo(new ChangeV1("one"));
        assertThat(rpc.decode(bytes("rpc-request.json"),ReadV1.class)).isEqualTo(new ReadV1("one"));
        assertThat(Contracts.inspect(ChangeV1.class,false).version()).isEqualTo(1);
        assertThat(Contracts.inspect(ReadV2.class,true).version()).isEqualTo(2);
        assertThat(rpc.decode(bytes("rpc-response.json"),ResponseV1.class)).isEqualTo(new ResponseV1("one"));
        assertThat(rpc.decode(bytes("rpc-response-v2.json"),ResponseV2.class)).isEqualTo(new ResponseV2("one","USD"));
        assertThatThrownBy(() -> rpc.decode(bytes("rpc-response-v2.json"),ResponseV1.class)).isInstanceOf(GatewayException.class);
        assertThatThrownBy(() -> rpc.decode(bytes("rpc-response.json"),ResponseV2.class)).isInstanceOf(GatewayException.class);
        assertThatThrownBy(() -> rpc.decode("{\"id\":\"one\",\"id\":\"two\"}".getBytes(StandardCharsets.UTF_8),ResponseV1.class))
                .isInstanceOf(GatewayException.class);
        byte[] stored=rpc.encode(new ResponseV1("one"),ResponseV1.class);
        assertThat(rpc.decode(stored,ResponseV1.class)).isEqualTo(new ResponseV1("one"));
        assertThatThrownBy(() -> rpc.decode(stored,ResponseV2.class)).isInstanceOf(GatewayException.class);
    }
    @Test void eventVersionsRouteToTheirDeclaredPayloadWithoutSchemaIdRemapping() throws Exception {
        var observed=new java.util.concurrent.atomic.AtomicReference<Object>();
        var first=handler(1,PayloadV1.class,observed);var second=handler(2,PayloadV2.class,observed);
        var registry=new com.apex.platform.messaging.consumer.DefaultEventHandlerRegistry(List.of(first,second));
        var executor=new com.apex.platform.messaging.consumer.EventHandlerExecutor() {
            public <T> com.apex.platform.messaging.consumer.DispatchResult execute(com.apex.platform.messaging.consumer.EventHandler<T> handler,
                    EventEnvelope<T> event,com.apex.platform.messaging.consumer.ConsumerContext context) {
                handler.handle(event,context);return com.apex.platform.messaging.consumer.DispatchResult.HANDLED;
            }
        };
        var context=new com.apex.platform.messaging.consumer.ConsumerContext("fixture","fixture",0,0,null,Instant.parse("2026-01-02T03:04:05Z"));
        var dispatcher=new com.apex.platform.messaging.jdbc.JacksonEventMessageDispatcher(mapper,registry,executor,true);
        dispatcher.dispatch(new String(bytes("event-envelope.json"),StandardCharsets.UTF_8),context);
        assertThat(observed.get()).isEqualTo(new PayloadV1("one"));
        dispatcher.dispatch(new String(bytes("event-envelope-v2.json"),StandardCharsets.UTF_8),context);
        assertThat(observed.get()).isEqualTo(new PayloadV2("one","USD"));
        assertThatThrownBy(()->dispatcher.dispatch(new String(bytes("event-envelope-v2.json"),StandardCharsets.UTF_8)
                .replace("\"eventVersion\":2","\"eventVersion\":3"),context))
                .isInstanceOf(com.apex.platform.messaging.consumer.UnsupportedEventContractException.class);
    }
    private <T> com.apex.platform.messaging.consumer.EventHandler<T> handler(int version,Class<T> payload,java.util.concurrent.atomic.AtomicReference<Object> observed) {
        return new com.apex.platform.messaging.consumer.EventHandler<>() {
            public String eventType(){return "SDK_CHANGED";} public int eventVersion(){return version;}
            public Class<T> payloadType(){return payload;}
            public void handle(EventEnvelope<T> event,com.apex.platform.messaging.consumer.ConsumerContext context){observed.set(event.payload());}
        };
    }
    @Test void protobufFieldNumbersServiceIdentityAndUnknownFieldsRemainCompatible() throws Exception {
        var descriptor=Invocation.getDescriptor();
        assertThat(descriptor.getFields()).extracting(com.google.protobuf.Descriptors.FieldDescriptor::getNumber)
                .containsExactly(1,2,3,4,5,6,7,8,9,10);
        assertThat(descriptor.getFields()).extracting(com.google.protobuf.Descriptors.FieldDescriptor::getName)
                .containsExactly("contract_type","contract_version","content_type","payload","request_id","idempotency_key",
                        "correlation_id","causation_id","origin_cell","target_cell");
        assertThat(Reply.getDescriptor().getFields()).extracting(com.google.protobuf.Descriptors.FieldDescriptor::getNumber)
                .containsExactly(1,2,3,4,5,6,7);
        assertThat(CellGatewayGrpc.SERVICE_NAME).isEqualTo("apex.cell.rpc.v1.CellGateway");
        assertThat(CellGatewayGrpc.getInvokeQueryMethod().getFullMethodName()).isEqualTo("apex.cell.rpc.v1.CellGateway/InvokeQuery");
        // Frozen v1 binary: contract_type=SDK_READ, contract_version=1; unknown field 99=7.
        byte[] old=HexFormat.of().parseHex("0a0853444b5f524541441001980607");
        var invocation=Invocation.parseFrom(old);
        assertThat(invocation.getContractType()).isEqualTo("SDK_READ");
        assertThat(invocation.getContractVersion()).isEqualTo(1);
        assertThat(invocation.getUnknownFields().getField(99).getVarintList()).containsExactly(7L);
        assertThat(invocation.toByteArray()).isEqualTo(old);
    }
    @Test void operationsV1AndV2KeepTheirExplicitSchemaBoundaries() throws Exception {
        var query=rpc.decode(bytes("operations-query.json"),OwnerSearch.Query.class);
        assertThat(rpc.decode(bytes("operations-query.json"),OwnerRecentSearch.Query.class).limit()).isEqualTo(16);
        assertThat(query.limit()).isEqualTo(16);assertThat(query.firstPage()).isEqualTo(query);
        assertThat(rpc.decode(rpc.encode(query,OwnerSearch.Query.class),OwnerSearch.Query.class)).isEqualTo(query);
        var manifest=rpc.decode(bytes("operations-manifest-v1.json"),ContractManifest.class);
        assertThat(manifest.schemaVersion()).isEqualTo(1);assertThat(manifest.grpcDependencies()).isEmpty();
        assertThat(rpc.decode(rpc.encode(manifest,ContractManifest.class),ContractManifest.class)).isEqualTo(manifest);
        String v2=new String(bytes("operations-manifest-v1.json"),StandardCharsets.UTF_8).replace("\"schemaVersion\":1","\"schemaVersion\":2");
        assertThat(rpc.decode(v2.getBytes(StandardCharsets.UTF_8),ContractManifest.class).schemaVersion()).isEqualTo(2);
        assertThatThrownBy(() -> rpc.decode(v2.replace("\"schemaVersion\":2","\"schemaVersion\":3").getBytes(StandardCharsets.UTF_8),ContractManifest.class))
                .isInstanceOf(GatewayException.class);
        assertThatThrownBy(() -> manifest.gaps().add("MUTATED")).isInstanceOf(UnsupportedOperationException.class);
    }
}
