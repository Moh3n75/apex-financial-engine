# Platform product acceptance

Java 21, Maven 3.9.x (tested 3.9.16), Windows PowerShell. Disposable native PostgreSQL
and embedded KRaft follow the existing fixtures; no Docker requirement or shared services.
Offline mode requires previously cached Maven/plugin/native dependencies. The repository's
local application YAMLs and inherited endpoint/credential overrides are not used for verification.

```powershell
$env:JAVA_HOME = '<Java 21 JDK>'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m35-6/verify-platform-product.ps1 -Offline -MavenCommand '<absolute mvn.cmd>'
```

Default is the **full gate**: install the SDK and HTTP capability with tests, run unchanged
M35 Identity acceptance and the M34 → M32 → M31 → M30/M29/M22 chain (with artifact-owner
receipt updates), then build/test/boot the external consumer. It includes real packaged
Gateway/Eureka/3 Order + 2 Inventory + 2 Payment processes, mTLS, retry/DLT/replay,
Identity two-JVM code/PKCE/refresh/JWKS, Financial isolated schema generation and selected
tests, and bounded Saga performance regression. No tests are disabled to consolidate modules.

`-Fast` runs only SDK/HTTP and minimal external acceptance. **It cannot establish full PASS.**
It intentionally omits capabilities needing artifacts installed by the full chain.

External consumer alone, after the full capability artifacts are installed:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m35-6/verify-external-consumer.ps1 -Offline -MavenCommand '<absolute mvn.cmd>'
```

This creates a new `apex-sdk-consumer-<random>` directory under the OS temp directory,
outside the repository. POM uses normal Boot parent and installed APEX coordinates,
no reactor relativePath or business implementation. Default and `http` profiles run the
four independent consumer tests, package a Boot JAR and actually launch/close its JVM;
HTTP uses an ephemeral loopback port and the shared filter. The `capabilities` profile
runs four more tests: real owning PostgreSQL + transactional Outbox → owned Kafka;
public Java local + real HTTP/2 remote query; conflicting discovery-provider refusal
before registry connection; and discovery against an actual separately packaged owned
Eureka process (without auto-registering the consumer). Its Spring Cloud BOM is explicit. Capability query test is
in-process contexts over a real socket, not a claim of separate-JVM external gRPC security;
the complete existing multi-JVM mTLS fixtures separately qualify that boundary.

Evidence is ignored under `out/m35-6`, plus existing acceptance directories. Build logs,
runtime dependency trees, JUnit XML, external absolute path, JAR SHA and packaged JVM
PID/exit markers are retained. Failed development logs are retained rather than overwritten
as green. Temp consumer sources/build outputs contain no credentials and are retained for
reproduction; all app contexts, broker and PostgreSQL resources are closed by their fixture.
Never commit `out/`, `target/`, test keys or logs. No published repository/release, real
Kubernetes cluster, production ingress/PKI, full Financial runtime or M36 is implied.

Artifact upgrade: old context artifact -> `apex-spring-boot-starter`; old WebMVC runtime
artifact -> `apex-starter-webmvc`. Keep Java imports. Upgrade an application's resolved
artifact set coherently; do not add obsolete cached SNAPSHOT JARs alongside the new owners.
The content-hashed M22/M26/M32/M34 receipts validate new module owners and the JDK API
dependency; their freshness and altered-source negative tests remain enforced.
