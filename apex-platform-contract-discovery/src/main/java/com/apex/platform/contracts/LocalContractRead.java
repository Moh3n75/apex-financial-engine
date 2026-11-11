package com.apex.platform.contracts;

import com.apex.operations.contracts.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Top-level to avoid Spring importing a nested controller as a second configuration bean. */
@RestController
@Profile("local-ops")
@ConditionalOnProperty(name="apex.contract-discovery.local-http.enabled",havingValue="true")
final class LocalContractRead {
  private final ContractDiscoveryProvider provider;
  LocalContractRead(ContractDiscoveryProvider provider,Environment env) {
    if(!java.util.Set.of("127.0.0.1","::1").contains(env.getProperty("server.address",""))
        ||!java.util.Set.of("local","local-dev").contains(env.getProperty("apex.contract-discovery.environment","")))
      throw new IllegalStateException("LOCAL_CONTRACT_READ_REQUIRES_EXPLICIT_LOOPBACK_AND_LOCAL_ENVIRONMENT");
    this.provider=provider;
  }
  @GetMapping("/ops/v1/contracts")
  ResponseEntity<ContractManifest> read(HttpServletRequest request) {
    if(!java.util.Set.of("127.0.0.1","::1","0:0:0:0:0:0:0:1").contains(request.getRemoteAddr()))throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(provider.discoverCommunication(new ContractDiscoveryQueryV2()));
  }
}
