"""Evaluate the preregistered experiment; never enables source scans or storage."""
from pathlib import Path
import csv
import hashlib
import json
import sys

root = Path(__file__).resolve().parents[2]
source = Path(sys.argv[1]).resolve()
run = Path(sys.argv[2]).resolve()
assert source.is_relative_to(root / 'out') and run.is_relative_to(root / 'out/ops03d')
policy_path = source / 'tools/ops03d/experimental-policy.json'
policy = json.loads(policy_path.read_text(encoding='utf-8-sig'))
gates = policy['experimentalGates']
windows = json.loads((source / 'out/ops03d/ops03d-load-windows.json').read_text())
checks = []
for n in (1, 5):
    enabled = windows[n]
    for field, prefixes in (('httpMs', 'http'), ('terminalSagaObservedMs', 'saga')):
        for percentile in ('p95', 'p99'):
            baseline = max(windows[n-1][field][percentile], windows[n+2][field][percentile])
            ratio = enabled[field][percentile] / baseline
            threshold = gates[prefixes+percentile.upper()+'Ratio']
            checks.append(dict(window=n, metric=field+'.'+percentile, value=ratio, limit=threshold, passed=ratio <= threshold))
    cpu = enabled['businessCpuNanos'] / max(windows[n-1]['businessCpuNanos'], windows[n+2]['businessCpuNanos'])
    checks.append(dict(window=n, metric='businessCpuRatio', value=cpu, limit=gates['businessCpuRatio'], passed=cpu <= gates['businessCpuRatio']))
    before = enabled['dbBefore']; after = enabled['dbAfter']
    reads = sum(max(0, b['blks_read']-a['blks_read']) for a,b in zip(before, after))
    checks.append(dict(window=n, metric='dbBlocksReadDelta', value=reads, limit=gates['dbBlocksReadIncreasePerWindow'], passed=reads <= gates['dbBlocksReadIncreasePerWindow']))
    locks = sum(b['lock_waits'] for b in after)
    checks.append(dict(window=n, metric='sampledUngrantedLocks', value=locks, limit=0, passed=locks == 0))

cost_path = run / 'native-evidence/apex-operations-owner-jdbc-ops03d-native-cost.csv'
cost = list(csv.DictReader(cost_path.open()))
for row in cost:
    for p in ('95', '99'):
        value = int(row['p'+p+'Micros']) / 1000
        limit = gates['nativeP'+p+'Millis']
        checks.append(dict(scenario=row['retained']+'/'+row['scenario'], metric='nativeP'+p+'Millis', value=value, limit=limit, passed=value < limit))
assert len(windows) == 8 and all(w['accepted'] == 30 and w['completed'] == 30 for w in windows)
missing = ['Complete DB process CPU/OS IO and lock wait distributions',
           'Packaged whole-process allocation and reader pool wait distributions',
           'Hot traffic + locked SQL/DB restart/KRaft-outage combination',
           'Bounded automatic full-walk reconciliation and projection write/restart cost',
           'Maximum owner/source cardinality and production shared-host confidence']
result = dict(policySha256=hashlib.sha256(policy_path.read_bytes()).hexdigest().upper(), checks=checks,
              measuredExperimentalGates='PASS' if all(c['passed'] for c in checks) else 'EXCEEDED',
              productionSla='INCONCLUSIVE', projection='BLOCKED_SOURCE_AND_RECONCILIATION_RISK_NOT_FULLY_QUALIFIED',
              sourceDecisions=[dict(owner=o, kinds=['OUTBOX_EVENT','KNOWN_GROUP_INBOX','WORKFLOW'] + (['REQUEST_RECEIPT'] if o=='order-cell' else []),
                                    decision='LIMITED', policy='EXPLICIT_ON_DEMAND_INDEXED_RECENT_ONLY_NO_AUTOMATIC_POLLING') for o in ['order-cell','inventory-cell','payment-cell']],
              notMeasuredOrNotVerified=missing)
(run / 'budget-decision.json').write_text(json.dumps(result, indent=2))
print(json.dumps({k:result[k] for k in ['measuredExperimentalGates','productionSla','projection']}))
