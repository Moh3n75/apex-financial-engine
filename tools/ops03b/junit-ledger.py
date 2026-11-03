"""Count only a caller-selected fresh qualification tree, including wrapper archives.

The fixed product wrapper prefixes archived XML with its module name. Those files
must be counted too, particularly after Maven clean removes a packaged IT report.
Exact classname#testcase-name deduplicates repeated builds; any failure is retained.
Usage: python tools/ops03b/junit-ledger.py <fresh-source-root> <evidence-directory>
"""
from pathlib import Path
import json
import sys
import xml.etree.ElementTree as ET

root = Path(sys.argv[1]).resolve()
out = Path(sys.argv[2]).resolve()
cases = {}
errors = []
reports = sorted(root.rglob('*TEST-*.xml'))
for report in reports:
    try:
        for case in ET.parse(report).iter('testcase'):
            identity = case.get('classname', '') + '#' + case.get('name', '')
            status = ('failed' if case.find('failure') is not None or case.find('error') is not None
                      else 'skipped' if case.find('skipped') is not None else 'passed')
            entry = cases.setdefault(identity, {'identity': identity, 'outcomes': [], 'reports': []})
            if status not in entry['outcomes']:
                entry['outcomes'].append(status)
            entry['reports'].append(str(report.relative_to(root)))
    except ET.ParseError:
        errors.append(str(report.relative_to(root)))
rows = sorted(cases.values(), key=lambda c: c['identity'])
summary = {'sourceRoots': [str(root)], 'xmlReports': len(reports),
           'distinctIdentities': len(rows), 'passedOnly': sum(c['outcomes'] == ['passed'] for c in rows),
           'anyFailed': sum('failed' in c['outcomes'] for c in rows),
           'anySkipped': sum('skipped' in c['outcomes'] for c in rows), 'xmlParseErrors': errors,
           'policy': 'Exact classname#testcase-name; includes module-prefixed fresh wrapper archives; any failure retained. Caller must establish fresh source provenance.'}
out.mkdir(parents=True, exist_ok=True)
(out / 'junit-ledger.json').write_text(json.dumps({'summary': summary, 'cases': rows}, indent=2))
(out / 'junit-summary.json').write_text(json.dumps(summary, indent=2))
print(json.dumps(summary))
sys.exit(1 if summary['anyFailed'] or errors else 0)
