package com.apex.identity;

import static org.assertj.core.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.mock.env.MockEnvironment;

@Tag("unit")
class IdentityPolicyTest {
  MockEnvironment environment() {
    return new MockEnvironment()
        .withProperty("apex.identity.tenant-id", UUID.randomUUID().toString())
        .withProperty("apex.identity.issuer", "https://identity.example")
        .withProperty("apex.identity.trusted-https-ingress", "true")
        .withProperty("apex.identity.key-file", "external")
        .withProperty("apex.identity.active-kid", "active");
  }

  @Test
  void requiresExplicitStableIssuer() {
    assertThatThrownBy(() -> IdentityPolicy.validate(new MockEnvironment()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void refusesOrdinaryPlaintext() {
    assertThatThrownBy(
            () ->
                IdentityPolicy.validate(
                    environment().withProperty("apex.identity.issuer", "http://identity.example")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void requiresTrustedTlsTermination() {
    assertThatThrownBy(
            () ->
                IdentityPolicy.validate(
                    environment().withProperty("apex.identity.trusted-https-ingress", "false")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void rejectsIssuerCredentialsAndPaths() {
    for (String uri :
        List.of(
            "https://user@identity.example",
            "https://identity.example/other",
            "https://identity.example?x=1"))
      assertThatThrownBy(
              () ->
                  IdentityPolicy.validate(environment().withProperty("apex.identity.issuer", uri)))
          .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void doesNotTrustForwardedAssertions() {
    assertThatThrownBy(
            () ->
                IdentityPolicy.validate(
                    environment().withProperty("server.forward-headers-strategy", "framework")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void explicitTlsPolicyAccepted() {
    assertThatCode(() -> IdentityPolicy.validate(environment())).doesNotThrowAnyException();
  }
}
