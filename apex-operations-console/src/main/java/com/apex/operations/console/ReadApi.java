package com.apex.operations.console;

import com.apex.operations.console.security.m36b.OpsM36bResourceAuthorizer;
import com.apex.operations.contracts.OwnerSnapshot.*;
import java.time.*;
import java.util.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/ops/v1")
public final class ReadApi {
  private final OwnerPoller sources;
  private final ObjectProvider<OpsM36bResourceAuthorizer> authorizer;

  public ReadApi(OwnerPoller sources, ObjectProvider<OpsM36bResourceAuthorizer> authorizer) {
    this.sources = sources;
    this.authorizer = authorizer;
  }

  public record Response(Instant asOf, String coverage, String uncertainty, Object data) {}

  private Response wrap(Object data) {
    return new Response(
        Instant.now(),
        "EXPLICIT_INVENTORY_PARTIAL_NO_HISTORY",
        "OTHER_CONSUMERS_BROKER_ACK_DLT_QUARANTINE_UNKNOWN",
        data);
  }

  private List<EvidenceCache.View> visibleViews() {
    var auth = authorizer.getIfAvailable();
    if (auth == null) {
      return sources.views();
    }
    String env = "test";
    return sources.views().stream().filter(v -> auth.ownerVisible(env, v.source())).toList();
  }

  @GetMapping("/overview")
  public Response overview() {
    return wrap(visibleViews());
  }

  @GetMapping("/cells")
  public Response cells(
      @RequestParam(defaultValue = "0") int offset, @RequestParam(defaultValue = "8") int limit) {
    if (offset < 0 || offset > 8 || limit < 1 || limit > 8)
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
    return wrap(visibleViews().stream().skip(offset).limit(limit).toList());
  }

  public record EventView(
      UUID id, List<EvidenceCache.View> sources, String brokerAck, String otherConsumers, String quarantine) {}

  @GetMapping("/events/{id}")
  public Response event(@PathVariable UUID id) {
    if (!sources.eventAllowed(id)) throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    return wrap(
        new EventView(
            id,
            select(id, null, null),
            "UNKNOWN_NO_VERIFIED_RECEIPT",
            "UNKNOWN",
            "UNKNOWN_NOT_INSPECTED"));
  }

  @GetMapping("/workflows/{owner}/{type}/{ref}")
  public Response workflow(
      @PathVariable String owner, @PathVariable String type, @PathVariable UUID ref) {
    var auth = authorizer.getIfAvailable();
    if (auth != null) {
      auth.require(
          "test",
          owner,
          "WORKFLOW",
          type,
          OpsM36bResourceAuthorizer.newCorrelationId());
    }
    if (!type.matches("[a-z][a-z-]{0,39}")
        || !sources.workflowAllowed(ref)
        || visibleViews().stream().noneMatch(v -> v.source().equals(owner)))
      throw new ResponseStatusException(HttpStatus.FORBIDDEN);
    return wrap(select(ref, owner, type));
  }

  private List<EvidenceCache.View> select(UUID id, String owner, String type) {
    return visibleViews().stream()
        .filter(v -> owner == null || owner.equals(v.source()))
        .map(
            v -> {
              var s = v.snapshot();
              if (s == null) return v;
              var filtered =
                  new com.apex.operations.contracts.OwnerSnapshot(
                      s.schemaVersion(),
                      s.environment(),
                      s.owner(),
                      s.instance(),
                      s.epoch(),
                      s.observedAt(),
                      s.maxAgeMillis(),
                      s.coverage(),
                      s.facts().stream()
                          .filter(
                              f ->
                                  f.ref().equals(id)
                                      && (type == null
                                          || (f.kind() == Kind.WORKFLOW && f.scope().equals(type))))
                          .toList(),
                      List.of(),
                      false,
                      0,
                      0);
              return new EvidenceCache.View(
                  v.source(), v.receivedAt(), filtered, v.freshness(), v.uncertainty(), List.of());
            })
        .toList();
  }

  public record ActiveView(
      String source,
      String freshness,
      Instant lastObservedAt,
      List<Attempt> attempts,
      boolean registryAvailable,
      long expired,
      long rejected,
      String coverage) {}

  @GetMapping("/operations/active")
  public Response active() {
    return wrap(
        visibleViews().stream()
            .map(
                v ->
                    new ActiveView(
                        v.source(),
                        v.freshness(),
                        v.snapshot() == null ? null : v.snapshot().observedAt(),
                        v.snapshot() != null && v.freshness().equals("FRESH")
                            ? v.snapshot().attempts()
                            : List.of(),
                        v.snapshot() != null && v.snapshot().activeAvailable(),
                        v.snapshot() == null ? 0 : v.snapshot().expiredAttempts(),
                        v.snapshot() == null ? 0 : v.snapshot().rejectedAttempts(),
                        "PARTIAL_MEMBERSHIP_TTL_EXPIRY_UNKNOWN_NO_COMPLETION"))
            .toList());
  }
}
