import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Standalone host diagnostic: no APEX, Spring, networking, database, or payloads. */
public class ScheduledTimerProbe {
  public static void main(String[] args) throws Exception {
    int iterations = args.length == 0 ? 1000 : Integer.parseInt(args[0]);
    if (args.length > 1 || iterations < 1 || iterations > 10000) {
      throw new IllegalArgumentException("Usage: ScheduledTimerProbe.java [iterations:1..10000]");
    }
    System.out.printf(
        "java=%s os=%s iterations=%d delayMs=25 observationTimeoutMs=1000%n",
        System.getProperty("java.runtime.version"), System.getProperty("os.name"), iterations);
    var scheduler =
        new ScheduledThreadPoolExecutor(
            1, Thread.ofPlatform().name("m33-standalone-timer").factory());
    scheduler.setRemoveOnCancelPolicy(true);
    try {
      for (int i = 0; i < iterations; i++) {
        long started = System.nanoTime();
        var completed = new CompletableFuture<Void>();
        var timer = scheduler.schedule(() -> completed.complete(null), 25, TimeUnit.MILLISECONDS);
        try {
          completed.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException failure) {
          System.err.printf(
              Locale.ROOT,
              "TIMEOUT iteration=%d elapsedMs=%.3f taskDelayMs=%d futureDone=%s active=%d queued=%d"
                  + " completed=%d%n",
              i,
              (System.nanoTime() - started) / 1_000_000.0,
              timer.getDelay(TimeUnit.MILLISECONDS),
              completed.isDone(),
              scheduler.getActiveCount(),
              scheduler.getQueue().size(),
              scheduler.getCompletedTaskCount());
          throw failure;
        }
        timer.cancel(false);
      }
      System.out.println("PASS: all timer observations completed within the unchanged bound");
    } finally {
      scheduler.shutdownNow();
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Owned timer thread did not stop");
      }
    }
  }
}
