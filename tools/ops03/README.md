# OPS-03 verification

Use Java 21 and Maven 3.9.x. The isolated wrapper uses a short owned out/o3-* source path to avoid Windows archive MAX_PATH failures and retains a clone of HEAD plus all current source overlays and all original logs; it never commits or stages. It avoids JARs locked by existing IDE/services. Excludes .git, target, out, .idea and private agent/AWS directories from the overlay. It does not delete source snapshots automatically.

```powershell
$env:JAVA_HOME='C:/Program Files/Java/jdk-21'
$env:Path="$env:JAVA_HOME/bin;$env:Path"
./tools/ops03/verify-isolated-ops03.ps1 -Offline -MavenCommand '<absolute mvn.cmd>'
```

Default Full runs OPS-03 focused/native PostgreSQL + packaged mTLS Console/Order tests, then the existing OPS-02 Full runner (including packaged measurements and fixed m35-6 full product gate/external consumer). Original failure logs are retained and nonzero exits propagate. `-Phase Focused` only qualifies the 12 OPS-03 cases, never the full regression or performance SLA. No passing production overhead verdict without agreed numeric budgets.

`verify-ops03.ps1` is the underlying in-place runner. It uses a separate `ops03` Order build directory. Avoid running it against source used by another build. Packaging and process tests use owned native PostgreSQL, ephemeral test certificates and child JVMs. Existing helpers clean only their own resources. The packaged OPS-03 fixture has no broker and applies explicit fixture-owned current-state updates; it does not claim Saga/Kafka qualification. Existing OPS-02 Full supplies its independent KRaft/business regression.

Start Console alone with `java -jar apex-operations-console/target/apex-operations-console-1.0-SNAPSHOT.jar`; without inventory it returns explicit partial empty/MISSING views and opens no owner transport. For actual owner evidence, use the explicit mTLS deployment properties documented in APEX_OPS_03_READ_ONLY_API_AND_SECURITY.md and the complete disposable example in Ops03ProcessIT. No configuration supplied to Console includes owner DB credentials. The owner handler defaults off and requires a separately configured owner read pool.

Current-slice limitations and exact acceptance are in the five root OPS-03 reports. There is no dashboard, public ingress, persistent Ops store, generic owner SQL API or action endpoint.

Fresh case ledger: `python tools/ops03/junit-ledger.py <fresh-source-root> [other-fresh-roots ...] <evidence-directory>`. It merges exact classname#testcase identities, records every report path and preserves any failed/skipped outcome. Use only fresh qualified trees, not old workspace target reports. The current acceptance composes the fixed full-product gate with current OPS-03 Focused qualification; see FINAL_ACCEPTANCE for the original wrapper path failure and exact source scopes.
