# Reviewed M30 baseline before Java-first gRPC

Date: 2026-10-27. Branch: credit-engine-event-sourcing.
Implementation commit supplied by the project owner:
`e0003cd2c14c11820433f07d1403bb41f6bb6327`.

Before M31 source changes, the complete existing command was executed with Java 21
and cached Maven 3.9.16:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/m30/verify-m30.ps1 -Offline -MavenCommand mvn.cmd
```

Exit 0, final build completed 2026-10-27 18:18:00 +03:30. Independent JUnit report
accounting: 291 distinct tests, zero failures/errors/skips. This includes isolated
Financial V1..V16 generation/test-compile + ten pure Financial tests, prior Platform,
Customer/Reporting/Order/Inventory/Payment/Replay regression, real packaged replay,
M29 operational recovery and M30 bounded three-Cell measurements. Logs and runtime
evidence are ignored under out/ and module target/ directories; they are not source.
Financial runtime integration, authenticated production deployment and capacity beyond
these bounded disposable fixtures are not certified.

At entry the index was empty and HEAD already contained the M26–M30 implementation.
Seven modified application/test YAMLs contain user-local connection settings and are
explicitly excluded, byte-preserved: apex-bootstrap main/test, apex-cell-template,
customer-cell, reporting-cell, order-cell and inventory-cell. Eleven older untracked
milestone reports remain outside this checkpoint as historical local artifacts.
The accepted M30 report is included as historical evidence; its earlier HEAD/PIDs
describe its own run, not the revalidation above. No application source, credentials
or infrastructure configuration is changed by this documentation checkpoint.

Only this file and APEX_M30_FINANCIAL_BUILD_PERFORMANCE_REPORT.md are staged for the
requested final checkpoint; no push or amendment is authorized. The reviewed result
is to receive the immutable local tag apex-m30-baseline after commit verification.
