package com.apex.operations.contracts;

import com.apex.platform.grpc.api.CellQuery;
import java.time.*;
import java.util.*;

/** Separate opt-in indexed recent metadata search. Source time is never commit time. */
public final class OwnerRecentSearch {
    private OwnerRecentSearch() {}
    public static final int MAX_ROWS = 16, MAX_PAGES = 8, MAX_REQUEST_BYTES = 1024, MAX_RESPONSE_BYTES = 16384;
    public enum Kind { OUTBOX_EVENT, KNOWN_GROUP_INBOX, REQUEST_RECEIPT, WORKFLOW }
    public enum Gap { PARTIAL_CURRENT_STATE, RETENTION_UNQUALIFIED, RECONCILE_FROM_START,
        CANDIDATE_PAGE_LIMIT, WALK_LIMIT, CLOCK_UNCERTAIN, INDEXED_RECENT_RANGE, STATE_POST_FILTERED, LATE_COMMIT_GAP, RESTART_WALK_REQUIRED }
    public enum Evidence { OWNER_COMMITTED_ROW }
    public enum Certainty { VERIFIED_WITHIN_SCOPE, UNKNOWN }

    @CellQuery(type="OPS_OWNER_RECENT_SEARCH", version=1, response=Page.class)
    public record Query(String environment, String owner, Kind kind, String scope,
                        Instant fromInclusive, Instant toExclusive, String state,
                        int limit, String cursor) {
        public Query {
            name(environment); name(owner); Objects.requireNonNull(kind);
            if (scope == null || !scope.matches("[A-Za-z0-9._-]{1,200}")) fail();
            if (fromInclusive == null || toExclusive == null || !fromInclusive.isBefore(toExclusive)
                    || Duration.between(fromInclusive,toExclusive).compareTo(Duration.ofHours(1)) > 0
                    || fromInclusive.isBefore(Instant.parse("2020-01-01T00:00:00Z"))
                    || toExclusive.isAfter(Instant.parse("2200-01-01T00:00:00Z"))) fail();
            if (state == null || !state.matches("[A-Z_]{1,40}") || limit < 1 || limit > MAX_ROWS) fail();
            if (cursor == null || cursor.length() > 256 || !cursor.matches("[A-Za-z0-9._-]*")) fail();
            if (512 + environment.length() + owner.length() + scope.length() + state.length() + cursor.length() > MAX_REQUEST_BYTES) fail();
        }
        public Query firstPage() { return new Query(environment,owner,kind,scope,fromInclusive,toExclusive,state,limit,""); }
        public String binding() { return "1|RECENT_DESC|"+environment+"|"+owner+"|"+kind+"|"+scope+"|"+fromInclusive+"|"+toExclusive+"|"+state+"|"+limit; }
    }
    public record Item(UUID ref, String state, Instant sourceAssignedAt, String evidenceKey,
                       Evidence evidenceClass, Certainty certainty) {
        public Item {
            Objects.requireNonNull(ref); Objects.requireNonNull(evidenceClass); Objects.requireNonNull(certainty);
            if (state == null || !state.matches("[A-Z_]{1,40}") || sourceAssignedAt == null
                    || evidenceKey == null || !evidenceKey.matches("[A-Z_]{1,24}:[a-f0-9-]{36}")) fail();
        }
    }
    public record Page(int schemaVersion, String environment, String owner, String instance, UUID epoch,
                       Kind kind, String scope, Instant fromInclusive, Instant toExclusive,
                       Instant scanStartedAt, Instant scanCompletedAt, Instant asOf, long maxAgeMillis,
                       int candidatesExamined, List<Gap> coverage, List<Item> items, String nextCursor,
                       String relation) {
        public Page {
            name(environment); name(owner); name(instance); Objects.requireNonNull(epoch); Objects.requireNonNull(kind);
            if (schemaVersion != 1 || maxAgeMillis != 10000 || scope == null || !scope.matches("[A-Za-z0-9._-]{1,200}")
                    || fromInclusive == null || toExclusive == null || scanStartedAt == null || scanCompletedAt == null
                    || asOf == null || !asOf.equals(scanStartedAt) || candidatesExamined < 0 || candidatesExamined > MAX_ROWS
                    || nextCursor == null || nextCursor.length() > 256 || !nextCursor.matches("[A-Za-z0-9._-]*")
                    || !"RELATION_UNKNOWN".equals(relation)) fail();
            coverage = List.copyOf(coverage); items = List.copyOf(items);
            if (items.size() > candidatesExamined || !coverage.contains(Gap.PARTIAL_CURRENT_STATE)
                    || !coverage.contains(Gap.RETENTION_UNQUALIFIED) || !coverage.contains(Gap.RECONCILE_FROM_START)) fail();
            var keys = new HashSet<String>();
            for (var item : items) {
                if (!item.evidenceKey().equals(kind+":"+item.ref()) || !keys.add(item.evidenceKey())
                        || item.sourceAssignedAt().isBefore(fromInclusive) || !item.sourceAssignedAt().isBefore(toExclusive)) fail();
            }
            // All exported strings are bounded ASCII; this exceeds their JSON escaping/field overhead.
            int upperBound = 2048 + nextCursor.length();
            for (var item : items) upperBound += 256 + 2*(item.evidenceKey().length()+item.state().length()+80);
            if (upperBound > MAX_RESPONSE_BYTES) fail();
        }
    }
    private static void name(String s) { if (s == null || !s.matches("[A-Za-z0-9._-]{1,64}")) fail(); }
    private static void fail() { throw new IllegalArgumentException("SEARCH_BOUND"); }
}
