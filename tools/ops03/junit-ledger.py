"""Count fresh JUnit identities once in a newly created source snapshot.

Usage: python tools/ops03/junit-ledger.py <fresh-source-root> [additional-fresh-roots ...] <evidence-directory>
Never point this at an old workspace to claim a fresh gate. Caller owns source provenance.
"""
from pathlib import Path
import json
import sys
import xml.etree.ElementTree as ET

roots = [Path(arg).resolve() for arg in sys.argv[1:-1]]
out = Path(sys.argv[-1]).resolve()
cases = {}
parse_errors = []
reports = [(root, report) for root in roots for report in root.rglob('TEST-*.xml')]
for root, report in reports:
    try:
        tree = ET.parse(report)
        for case in tree.iter('testcase'):
            identity = case.get('classname', '') + '#' + case.get('name', '')
            status = 'failed' if case.find('failure') is not None or case.find('error') is not None else 'skipped' if case.find('skipped') is not None else 'passed'
            entry = cases.setdefault(identity, {'identity': identity, 'outcomes': [], 'reports': []})
            if status not in entry['outcomes']:
                entry['outcomes'].append(status)
            entry['reports'].append(str(root) + '::' + str(report.relative_to(root)))
    except ET.ParseError:
        parse_errors.append(str(root) + '::' + str(report.relative_to(root)))
rows = sorted(cases.values(), key=lambda c: c['identity'])
summary = {'sourceRoots': [str(root) for root in roots], 'xmlReports': len(reports), 'distinctIdentities': len(rows),
           'passedOnly': sum(c['outcomes'] == ['passed'] for c in rows),
           'anyFailed': sum('failed' in c['outcomes'] for c in rows),
           'anySkipped': sum('skipped' in c['outcomes'] for c in rows), 'xmlParseErrors': parse_errors,
           'policy': 'Exact classname#testcase-name; repeated modes/rebuilds count once. Any failure remains visible. Fresh-source provenance is required separately.'}
out.mkdir(parents=True, exist_ok=True)
(out / 'junit-ledger.json').write_text(json.dumps({'summary': summary, 'cases': rows}, indent=2))
(out / 'junit-summary.json').write_text(json.dumps(summary, indent=2))
print(json.dumps(summary))
sys.exit(1 if summary['anyFailed'] or parse_errors else 0)
