package com.apex.identity;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.*;
import com.nimbusds.jose.jwk.source.*;
import com.nimbusds.jose.proc.SecurityContext;
import java.nio.file.*;
import java.util.*;
import org.springframework.core.env.Environment;

final class SigningKeys {
  final RSAKey active;
  final JWKSet publicKeys;

  SigningKeys(Environment e) throws Exception {
    Path path = Path.of(e.getRequiredProperty("apex.identity.key-file"));
    if (!Files.isRegularFile(path) || Files.size(path) > 65536)
      throw new IllegalStateException("Bounded external signing key set required");
    java.util.List<JWK> keys;
    try {
      keys = JWKSet.parse(Files.readString(path)).getKeys();
    } catch (java.text.ParseException invalid) {
      throw new IllegalStateException("Invalid external signing set");
    }
    var ids = new HashSet<String>();
    var activeId = e.getRequiredProperty("apex.identity.active-kid");
    RSAKey signing = null;
    for (var key : keys) {
      if (!(key instanceof RSAKey rsa)
          || rsa.size() < 2048
          || key.getKeyID() == null
          || !key.getKeyID().matches("[A-Za-z0-9_-]{1,80}")
          || !ids.add(key.getKeyID())
          || !JWSAlgorithm.RS256.equals(key.getAlgorithm())
          || !KeyUse.SIGNATURE.equals(key.getKeyUse()))
        throw new IllegalStateException("Unique RS256 signing identities required");
      if (activeId.equals(key.getKeyID()) && rsa.isPrivate()) signing = rsa;
      else if (rsa.isPrivate())
        throw new IllegalStateException("Only active key may contain private material");
    }
    if (signing == null || keys.size() > 4)
      throw new IllegalStateException("Stable active private key required");
    active = signing;
    publicKeys = new JWKSet(keys.stream().map(JWK::toPublicJWK).toList());
  }

  JWKSource<SecurityContext> publicSource() {
    return new ImmutableJWKSet<>(publicKeys);
  }

  JWKSource<SecurityContext> signingSource() {
    return new ImmutableJWKSet<>(new JWKSet(active));
  }
}
