# M33 gate and timing diagnostic

M33 performance work is **blocked at Gate A** by an intermittent unchanged M32
deadline test. No Outbox setting, batch size, send concurrency or delivery behavior
has been changed. There is no claimed M33 load baseline or optimization result.

Run the full prerequisite from the repository root with Java 21 and cached Maven:

```powershell
$env:JAVA_HOME = '<installed Java 21 JDK>'
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File tools/m32/verify-m32.ps1 -Offline -MavenCommand mvn.cmd
```

This is the full default verifier, including isolated Financial compile, Saga,
Replay and the actual packaged mTLS collector acceptance. Do not use `-Focused`,
skip tests, or infer acceptance from an earlier green report. If it fails, preserve
its log, JUnit XML and `apex-platform-grpc/target/m32-deadline-threads.log` before a
clean build. Stop before changing performance settings.

The standalone diagnostic reproduces the timing boundary without APEX or services:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass `
  -File tools/m33/diagnose-timers.ps1 -Iterations 1000
```

It schedules sequential 25 ms tasks, observes each with the original one-second
bound, and prints elapsed time, overdue delay and executor state on failure.
Iterations are bounded to 1..10000; 1000 takes approximately 25 seconds on a normally
scheduled host. A timeout exits nonzero. A green sample does not establish that an
intermittent problem is fixed. The probe owns and shuts down its single thread. It
requires no Spring, Maven artifact, broker, database, credentials or external network.

Once the timing failure is explained or the execution environment is independently
qualified, rerun full Gate A. Only then instrument and compare M30's actual packaged
10 RPS moderate / 30 RPS hotspot / 60 RPS spike with identical durable DB settings,
traffic and instrumentation. Claim time, serial send-to-ACK, published-mark time and
fixed-delay idle time must be measured before selecting one opt-in adjustment.
There is no A/B runner yet: the user explicitly required stopping before that work
if full M32 acceptance does not pass. See the two root-level acceptance reports.
