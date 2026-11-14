"""Audit current SDK ownership, registrations and declared dependency graph."""
from pathlib import Path
import json
import xml.etree.ElementTree as E

root = Path(__file__).resolve().parents[2]
ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
before = json.loads((root / 'docs/m37-audit.json').read_text())
obsolete = set(before['mapping'])
modules = sorted(p.parent for p in root.glob('apex-sdk-*/pom.xml'))
graph = {}
registrations = {}
for module in modules:
    tree = E.parse(module / 'pom.xml')
    assert tree.findtext('m:parent/m:version', namespaces=ns) == '1.0-SNAPSHOT'
    graph[module.name] = []
    for dep in tree.findall('m:dependencies/m:dependency', ns):
        artifact = dep.findtext('m:artifactId', namespaces=ns)
        assert artifact not in obsolete, artifact
        graph[module.name].append({
            'artifact': artifact,
            'scope': dep.findtext('m:scope', 'compile', ns),
            'optional': dep.findtext('m:optional', 'false', ns) == 'true'})
    for imports in (module / 'src/main/resources').glob('META-INF/spring/*.imports'):
        for name in imports.read_text().splitlines():
            if not name or name.startswith('#'):
                continue
            assert name not in registrations, name
            assert (module / 'src/main/java' / (name.replace('.', '/') + '.java')).is_file(), name
            registrations[name] = module.name

def closure(name, seen=None):
    seen = set() if seen is None else seen
    if name in seen:
        return seen
    seen.add(name)
    for dep in graph.get(name, []):
        if dep['scope'] not in ('test', 'provided') and not dep['optional']:
            closure(dep['artifact'], seen)
    return seen

for entry in ('apex-sdk-spring-boot', 'apex-sdk-operations'):
    forbidden = {'spring-boot-starter-kafka', 'postgresql', 'spring-jdbc',
                 'spring-boot-starter-webmvc', 'spring-boot-starter-grpc-server', 'kubernetes-client'}
    assert not closure(entry).intersection(forbidden), entry
assert 'spring-boot-starter-grpc-server' not in closure('apex-sdk-grpc-client')
assert 'spring-boot-starter-grpc-client' not in closure('apex-sdk-grpc-server')
consumer_poms = [root / name for name in before['before'] if (root / name).is_file()]
consumers = {}
for pom in consumer_poms + [module / 'pom.xml' for module in modules]:
    edges = []
    for dep in E.parse(pom).findall('.//m:dependency', ns):
        artifact = dep.findtext('m:artifactId', namespaces=ns)
        assert artifact not in obsolete, str(pom)
        if artifact.startswith('apex-sdk-'):
            edges.append({'artifact': artifact, 'scope': dep.findtext('m:scope', 'compile', ns),
                          'version': dep.findtext('m:version', namespaces=ns)})
    if pom in consumer_poms:
        consumers[str(pom.relative_to(root)).replace('\\', '/')] = edges
report = {'beforeArtifactCount': len(obsolete), 'afterArtifactCount': len(modules),
          'dependencies': graph, 'consumersIncludingProfiles': consumers, 'autoConfigurations': registrations,
          'runtimeClosures': {name: sorted(closure(name)) for name in graph}}
(root / 'docs/m37-after.json').write_text(json.dumps(report, indent=2) + '\n')
print(f'SDK audit PASS: {len(obsolete)} -> {len(modules)} artifacts; '
      f'{len(registrations)} unique auto-configuration registrations.')
