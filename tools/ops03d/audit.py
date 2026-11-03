"""Verify entry bytes, exact frozen inputs, Git preservation and ignored evidence hashes."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys
root = Path(__file__).resolve().parents[2]
source = Path(sys.argv[1]).resolve(); run = Path(sys.argv[2]).resolve()
evidence = root / 'out/ops03d'
assert source.is_relative_to(root / 'out') and run.is_relative_to(evidence)
baseline = json.loads((evidence/'baseline.json').read_text(encoding='utf-8-sig'))
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest().upper()
def git(*args):return subprocess.check_output(['git','-C',str(root),*args],text=True).splitlines()
changed=[]
for row in baseline['files']:
    path=root/row['path'];assert path.is_file(),row['path']
    assert sha(evidence/'entry'/row['path'])==row['sha256']
    if sha(path)!=row['sha256']:changed.append(dict(path=row['path'],entrySha256=row['sha256'],finalSha256=sha(path)))
assert {r['path'] for r in changed}=={'apex-operations-console/src/main/java/com/apex/operations/console/ReadOnlyFilter.java'}
assert git('rev-parse','HEAD')[0]==baseline['head'] and git('ls-files','--stage')==baseline['index']
frozen=json.loads((run/'tested-source.json').read_text(encoding='utf-8-sig'))
for row in frozen:assert sha(root/row['path'])==row['sha256']==sha(source/row['path']),row['path']
files=[dict(path=p,sha256=sha(root/p)) for p in git('ls-files','--cached','--others','--exclude-standard') if (root/p).is_file()]
entry={r['path'] for r in baseline['files']}; new=sorted(r['path'] for r in files if r['path'] not in entry)
guide=(root/'tools/ops03d/stage-manifest.txt').read_text().splitlines()
assert all(p in guide for p in new),[p for p in new if p not in guide]
preservation=dict(head=baseline['head'],branch=git('branch','--show-current')[0],entryFiles=len(entry),changedEntryFiles=changed,indexUnchanged=True,
                  status=git('status','--porcelain=v1','--untracked-files=all'),frozenInputs=len(frozen),sourceDrift=[])
(evidence/'preservation.json').write_text(json.dumps(preservation,indent=2))
(evidence/'final-source-sha256.json').write_text(json.dumps(dict(head=baseline['head'],files=files,newPaths=new),indent=2))
runtime=set()
for folder in (evidence,source/'out/ops03d',source/'out/ops03c',source/'out/ops03b',source/'out/m35-6'):
    if folder.exists():runtime.update(p for p in folder.rglob('*') if p.is_file() and not p.is_relative_to(evidence/'entry') and p.suffix.lower() in {'.json','.xml','.csv','.txt','.log'})
runtime.update(source.rglob('*TEST-*.xml'))
runtime.discard(evidence/'runtime-evidence-sha256.json')
(evidence/'runtime-evidence-sha256.json').write_text(json.dumps([dict(path=str(p.relative_to(root)),bytes=p.stat().st_size,sha256=sha(p)) for p in sorted(runtime)],indent=2))
print(json.dumps(dict(entryFiles=len(entry),changedEntryPaths=[r['path'] for r in changed],frozenInputs=len(frozen),newPaths=len(new),hashedEvidenceFiles=len(runtime),headAndIndexUnchanged=True)))
