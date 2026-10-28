package com.apex.platform.grpc.internal;

import static org.assertj.core.api.Assertions.*;

import com.apex.platform.grpc.api.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

@Tag("unit")
class ContractValidationTest {
  @CellRequest(type = "WRITE", version = 1, response = Answer.class)
  public record Write(String text) {}

  @CellQuery(type = "READ", version = 1, response = Answer.class)
  public record Read(String text) {}

  public record Answer(String text) {}

  @CellQuery(type = "", version = 0, response = Answer.class)
  public record Invalid(String text) {}

  @CellQuery(type = "BAD", version = 1, response = Answer.class)
  public record Unsafe(Object value) {}

  public static class Good {
    @CellQueryHandler(type = "READ", version = 1)
    public Answer read(Read q) {
      return new Answer(q.text());
    }
  }

  public static class Wrong {
    @CellQueryHandler(type = "READ", version = 1)
    public Read read(Read q) {
      return q;
    }
  }

  public static class Signature {
    @CellQueryHandler(type = "READ", version = 1)
    public Answer read() {
      return null;
    }
  }

  HandlerRegistry registry(Object... beans) {
    var factory = new DefaultListableBeanFactory();
    for (int i = 0; i < beans.length; i++) factory.registerSingleton("handler" + i, beans[i]);
    return new HandlerRegistry(factory);
  }

  @Test
  void validatesConcreteRecordsAndResponseDeclaration() {
    assertThat(Contracts.inspect(Read.class, true).response()).isEqualTo(Answer.class);
    assertThatThrownBy(() -> Contracts.inspect(Invalid.class, true))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Contracts.inspect(Unsafe.class, true))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Contracts.inspect(Write.class, true))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void detectsDuplicateHandlers() {
    var r = registry(new Good(), new Good());
    assertThatThrownBy(r::afterSingletonsInstantiated)
        .hasMessageContaining("Duplicate RPC handler")
        .hasMessageContaining("handler");
  }

  @Test
  void rejectsBadSignatureAndReturnType() {
    var a = registry(new Wrong());
    assertThatThrownBy(a::afterSingletonsInstantiated).hasMessageContaining("response mismatch");
    var b = registry(new Signature());
    assertThatThrownBy(b::afterSingletonsInstantiated)
        .hasMessageContaining("optional RequestContext");
  }

  @Test
  void strictCodecAndCanonicalFingerprint() {
    var codec = new RpcCodec(128);
    assertThat(
            codec.fingerprint(
                codec.decode("{ \"text\" : \"hello\" }".getBytes(), Write.class), Write.class))
        .isEqualTo(codec.fingerprint(new Write("hello"), Write.class));
    assertThatThrownBy(() -> codec.decode("{\"text\":\"a\",\"other\":1}".getBytes(), Write.class))
        .isInstanceOf(GatewayException.class);
    assertThatThrownBy(() -> codec.encode(new Write("x".repeat(200)), Write.class))
        .isInstanceOf(GatewayException.class);
    assertThatThrownBy(
            () -> codec.decode("{\"text\":\"a\",\"text\":\"b\"}".getBytes(), Write.class))
        .isInstanceOf(GatewayException.class);
  }

  @Test
  void optionsEnforceFiniteBoundsAndStableIdentity() {
    var a = RequestOptions.withIdempotencyKey("key");
    assertThat(a.withDeadline(Duration.ofMillis(100)).requestId()).isEqualTo(a.requestId());
    assertThatThrownBy(() -> a.withDeadline(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RequestOptions.withIdempotencyKey("bad key"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  public static class NullReply {
    @CellQueryHandler(type = "READ", version = 1)
    public Answer read(Read q) {
      return null;
    }
  }

  public static class InvalidMetadata {
    @CellQueryHandler(type = "READ", version = 1)
    public Answer read(Read q, String context) {
      return null;
    }
  }

  public interface QueryService {
    Answer read(Read q);
  }

  public static class Proxied implements QueryService {
    @CellQueryHandler(type = "READ", version = 1)
    public Answer read(Read q) {
      return new Answer(q.text());
    }
  }

  @Test
  void rejectsInvalidContextAndMissingRegistrations() {
    var r = registry(new InvalidMetadata());
    assertThatThrownBy(r::afterSingletonsInstantiated)
        .hasMessageContaining("handler0")
        .hasMessageContaining("optional RequestContext");
    var empty = registry();
    empty.afterSingletonsInstantiated();
    assertThatThrownBy(() -> empty.find(true, "READ", 99))
        .isInstanceOfSatisfying(
            GatewayException.class,
            e -> assertThat(e.code()).isEqualTo(GatewayException.Code.UNIMPLEMENTED));
  }

  @Test
  void invokesJdkAndCglibSpringProxyAdviceWithoutBypassingTarget() throws Throwable {
    for (boolean cglib : List.of(false, true)) {
      var advice = new java.util.concurrent.atomic.AtomicInteger();
      var proxy = new org.springframework.aop.framework.ProxyFactory(new Proxied());
      proxy.setProxyTargetClass(cglib);
      proxy.addAdvice(
          (org.aopalliance.intercept.MethodInterceptor)
              invocation -> {
                advice.incrementAndGet();
                return invocation.proceed();
              });
      var r = registry(proxy.getProxy());
      r.afterSingletonsInstantiated();
      assertThat(r.find(true, "READ", 1).invoke(new Read("proxy"), null))
          .isEqualTo(new Answer("proxy"));
      assertThat(advice.get()).isEqualTo(1);
    }
  }

  @Test
  void codecRejectsNullResponseMalformedAndOverNestedPayload() {
    var codec = new RpcCodec(65536);
    assertThatThrownBy(() -> codec.encode(null, Answer.class))
        .isInstanceOfSatisfying(
            GatewayException.class,
            e -> assertThat(e.code()).isEqualTo(GatewayException.Code.INTERNAL));
    assertThatThrownBy(() -> codec.decode("null".getBytes(), Read.class))
        .isInstanceOf(GatewayException.class);
    assertThatThrownBy(
            () -> codec.decode(("[".repeat(20) + "0" + "]".repeat(20)).getBytes(), Read.class))
        .isInstanceOf(GatewayException.class);
    assertThatThrownBy(() -> codec.decode("{broken".getBytes(), Read.class))
        .isInstanceOf(GatewayException.class);
  }
}
