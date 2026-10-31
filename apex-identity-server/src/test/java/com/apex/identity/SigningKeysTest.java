package com.apex.identity;

import static org.assertj.core.api.Assertions.*;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

@Tag("unit")
class SigningKeysTest {
  @TempDir Path temporary;
  static RSAKey a, b;

  @BeforeAll
  static void keys() throws Exception {
    a =
        new RSAKeyGenerator(2048)
            .keyID("a")
            .keyUse(KeyUse.SIGNATURE)
            .algorithm(JWSAlgorithm.RS256)
            .generate();
    b =
        new RSAKeyGenerator(2048)
            .keyID("b")
            .keyUse(KeyUse.SIGNATURE)
            .algorithm(JWSAlgorithm.RS256)
            .generate();
  }

  MockEnvironment config(List<JWK> keys, String active) throws Exception {
    Path path = temporary.resolve("set.json");
    Files.writeString(path, new JWKSet(keys).toString(false));
    return new MockEnvironment()
        .withProperty("apex.identity.key-file", path.toString())
        .withProperty("apex.identity.active-kid", active);
  }

  @Test
  void requiresAnExistingExternalSet() {
    assertThatThrownBy(
            () ->
                new SigningKeys(
                    new MockEnvironment()
                        .withProperty(
                            "apex.identity.key-file", temporary.resolve("absent").toString())))
        .isInstanceOf(Exception.class);
  }

  @Test
  void refusesPublicOnlyActiveKey() throws Exception {
    var e = config(List.of(a.toPublicJWK()), "a");
    assertThatThrownBy(() -> new SigningKeys(e)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void refusesPrivateInactiveKey() throws Exception {
    var e = config(List.of(a, b), "a");
    assertThatThrownBy(() -> new SigningKeys(e)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void refusesDuplicateKid() throws Exception {
    var e = config(List.of(a, a.toPublicJWK()), "a");
    assertThatThrownBy(() -> new SigningKeys(e)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void exportsOnlyPublicMaterialWithStableKids() throws Exception {
    var keys = new SigningKeys(config(List.of(a, b.toPublicJWK()), "a"));
    assertThat(keys.active.getKeyID()).isEqualTo("a");
    assertThat(keys.publicKeys.getKeys()).allMatch(k -> !k.isPrivate());
    assertThat(keys.publicKeys.getKeys().stream().map(JWK::getKeyID)).containsExactly("a", "b");
  }
}
