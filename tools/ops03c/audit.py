"""Read-only Git/source/evidence audit. Run after a fresh OPS-03C wrapper finishes."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

root = Path(__file__).resolve().parents[2]
source = Path(sys.argv[1]).resolve()
run = Path(sys.argv[2]).resolve()
assert source.is_relative_to(root / 'out') and run.is_relative_to(root / 'out/ops03c')
evidence = root / 'out/ops03c'
baseline = json.loads((evidence / 'baseline.json').read_text(encoding='utf-8-sig'))

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest().upper()

def git(*args):
    return subprocess.check_output(['git', '-C', str(root), *args], text=True).splitlines()

changed, missing = [], []
for row in baseline['files']:
    path = root / row['path']
    if not path.is_file():
        missing.append(row['path'])
    elif sha(path) != row['sha256']:
        changed.append({'path': row['path'], 'entrySha256': row['sha256'], 'finalSha256': sha(path)})
preservation = {'head': git('rev-parse', 'HEAD')[0], 'branch': git('branch', '--show-current')[0],
                'indexUnchanged': git('ls-files', '--stage') == baseline['index'],
                'entryFiles': len(baseline['files']), 'missing': missing, 'changedEntryFiles': changed,
                'status': git('status', '--porcelain=v1', '--untracked-files=all')}
(evidence / 'preservation.json').write_text(json.dumps(preservation, indent=2))
assert preservation['head'] == baseline['head'] and preservation['indexUnchanged'] and not missing
assert {row['path'] for row in changed} == {'apex-operations-console/src/main/java/com/apex/operations/console/ReadOnlyFilter.java'}

frozen = json.loads((run / 'tested-source.json').read_text(encoding='utf-8-sig'))
drift = []
for row in frozen:
    if sha(root / row['path']) != row['sha256'] or sha(source / row['path']) != row['sha256']:
        drift.append(row['path'])
(evidence / 'frozen-input-check.json').write_text(json.dumps({'inputs': len(frozen), 'changed': drift, 'excluded': 'Only new root OPS-03C post-run acceptance documents'}, indent=2))
assert not drift, drift

paths = git('ls-files', '--cached', '--others', '--exclude-standard')
rows = [{'path': p, 'sha256': sha(root / p)} for p in paths if (root / p).is_file()]
entry = {row['path'] for row in baseline['files']}
new = sorted(row['path'] for row in rows if row['path'] not in entry)
guide = (root / 'tools/ops03c/stage-manifest.txt').read_text().splitlines()
assert all(p in guide for p in new), [p for p in new if p not in guide]
(evidence / 'final-source-sha256.json').write_text(json.dumps({'head': preservation['head'], 'files': rows, 'newPaths': new}, indent=2))

runtime = set()
for folder in (run, source / 'out/ops03c', source / 'out/ops03b', source / 'out/m35-6'):
    if folder.exists():
        runtime.update(p for p in folder.rglob('*') if p.is_file() and p.suffix.lower() in {'.json', '.csv', '.log', '.txt', '.xml'})
runtime.update(source.rglob('*TEST-*.xml'))
manifest = [{'path': str(p.relative_to(root)), 'bytes': p.stat().st_size, 'sha256': sha(p)} for p in sorted(runtime)]
(evidence / 'runtime-evidence-sha256.json').write_text(json.dumps(manifest, indent=2))
print(json.dumps({'entryFiles': len(baseline['files']), 'changedEntryFiles': [r['path'] for r in changed], 'headAndIndexUnchanged': True,
                  'frozenInputs': len(frozen), 'sourceDrift': drift, 'newFiles': len(new), 'hashedRuntimeFiles': len(manifest)}))
