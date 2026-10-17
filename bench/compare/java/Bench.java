import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Java side of the Java vs Go Aeron client comparison. Same methodology as
 * bench/compare/gobench/main.go; keep the two in step.
 *
 * Both attach to an already running media driver, use aeron:ipc, and put the
 * subscriber behind a FragmentAssembler so a message is counted once however
 * many frames it took.
 */
public final class Bench
{
    static final String CHANNEL = "aeron:ipc";
    static final int PING = 1001;
    static final int PONG = 1002;
    static final int FRAGMENT_LIMIT = 10;

    static volatile boolean stop;
    static int pinProducer = -1;
    static int pinConsumer = -1;
    static boolean backoff;

    public static void main(final String[] args) throws Exception
    {
        final String dir = arg(args, "dir", "/dev/shm/aeron-bench");
        final String mode = arg(args, "mode", "all");
        final int[] sizes = Arrays.stream(arg(args, "sizes", "32,256,1024,4000").split(",")).mapToInt(Integer::parseInt).toArray();
        final int count = Integer.parseInt(arg(args, "count", "2000000"));
        final int repeats = Integer.parseInt(arg(args, "repeats", "5"));
        final int latCount = Integer.parseInt(arg(args, "latcount", "200000"));
        final int latWarmup = Integer.parseInt(arg(args, "latwarmup", "50000"));
        backoff = arg(args, "idle", "spin").equals("backoff");
        final String pin = arg(args, "pin", "");
        if (!pin.isEmpty())
        {
            final String[] cpus = pin.split(",");
            pinProducer = Integer.parseInt(cpus[0]);
            pinConsumer = Integer.parseInt(cpus[1]);
        }

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir)))
        {
            if (mode.equals("all") || mode.equals("throughput"))
            {
                for (final int size : sizes)
                {
                    throughput(aeron, size, count, repeats);
                }
            }
            if (mode.equals("burst"))
            {
                for (final int size : sizes)
                {
                    burst(aeron, size, repeats);
                }
            }
            if (mode.equals("all") || mode.equals("latency"))
            {
                for (final int size : sizes)
                {
                    latency(aeron, size, latCount, latWarmup);
                }
            }
        }
    }

    /** Restricts the calling thread to one CPU, as the Go harness does with sched_setaffinity. */
    static void pinThread(final int cpu)
    {
        if (cpu < 0)
        {
            return;
        }
        try
        {
            final String tid = new File("/proc/thread-self").getCanonicalFile().getName();
            final Process p = new ProcessBuilder("taskset", "-pc", Integer.toString(cpu), tid).redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            if (p.waitFor() != 0)
            {
                throw new IllegalStateException("taskset failed");
            }
        }
        catch (final Exception e)
        {
            throw new IllegalStateException("could not pin thread to cpu " + cpu, e);
        }
    }

    static String arg(final String[] args, final String key, final String def)
    {
        for (final String a : args)
        {
            if (a.startsWith("-" + key + "="))
            {
                return a.substring(key.length() + 2);
            }
        }
        return def;
    }

    static void await(final Publication pub)
    {
        final long deadline = System.nanoTime() + 10_000_000_000L;
        while (!pub.isConnected())
        {
            if (System.nanoTime() > deadline)
            {
                throw new IllegalStateException("publication not connected");
            }
            Thread.onSpinWait();
        }
    }

    static void offer(final Publication pub, final UnsafeBuffer buf, final int len)
    {
        long r;
        while ((r = pub.offer(buf, 0, len)) < 0)
        {
            if (r == Publication.CLOSED || r == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new IllegalStateException("offer failed: " + r);
            }
            Thread.onSpinWait();
        }
    }

    /** One publisher, one polling thread; time from first offer until every message is received. */
    static void throughput(final Aeron aeron, final int size, final int count, final int repeats) throws Exception
    {
        try (Publication pub = aeron.addPublication(CHANNEL, PING);
            Subscription sub = aeron.addSubscription(CHANNEL, PING))
        {
            await(pub);
            final UnsafeBuffer src = new UnsafeBuffer(ByteBuffer.allocateDirect(size));
            for (int run = 0; run <= repeats; run++) // run 0 is a discarded warm-up
            {
                final long[] received = new long[1];
                final FragmentAssembler asm = new FragmentAssembler((b, o, l, h) -> received[0]++);
                final Thread consumer = new Thread(() ->
                {
                    pinThread(pinConsumer);
                    final IdleStrategy idler = backoff ? new BackoffIdleStrategy() : NoOpIdleStrategy.INSTANCE;
                    while (received[0] < count)
                    {
                        idler.idle(sub.poll(asm, FRAGMENT_LIMIT));
                    }
                });
                consumer.start();
                pinThread(pinProducer);

                final long t0 = System.nanoTime();
                for (int i = 0; i < count; i++)
                {
                    offer(pub, src, size);
                }
                consumer.join();
                final long ns = System.nanoTime() - t0;

                if (run > 0)
                {
                    final double secs = ns / 1e9;
                    System.out.printf("RESULT lang=java mode=throughput size=%d run=%d msgs=%d msgs_per_sec=%.0f mb_per_sec=%.1f%n",
                        size, run, count, count / secs, (double)count * size / secs / 1e6);
                }
            }
        }
    }

    /**
     * Isolates the two halves of the pipeline: times a publisher filling the log with no consumer running
     * (a burst small enough to stay inside the flow-control window), then a consumer on another CPU
     * draining it with no publisher running.
     */
    static void burst(final Aeron aeron, final int size, final int repeats) throws Exception
    {
        try (Publication pub = aeron.addPublication(CHANNEL, PING);
            Subscription sub = aeron.addSubscription(CHANNEL, PING))
        {
            await(pub);
            final int n = 16 * 1024 * 1024 / (size + 32);
            final UnsafeBuffer src = new UnsafeBuffer(ByteBuffer.allocateDirect(size));
            pinThread(pinProducer);
            for (int run = 0; run <= repeats; run++) // run 0 is a discarded warm-up
            {
                final long flt0 = minorFaults();
                final long t0 = System.nanoTime();
                for (int i = 0; i < n; i++)
                {
                    offer(pub, src, size);
                }
                final double pubSecs = (System.nanoTime() - t0) / 1e9;
                final long pubFaults = minorFaults() - flt0;

                final double[] drainSecs = new double[1];
                final Thread consumer = new Thread(() ->
                {
                    pinThread(pinConsumer);
                    final long[] got = new long[1];
                    final FragmentAssembler asm = new FragmentAssembler((b, o, l, h) -> got[0]++);
                    final long t1 = System.nanoTime();
                    while (got[0] < n)
                    {
                        sub.poll(asm, FRAGMENT_LIMIT);
                    }
                    drainSecs[0] = (System.nanoTime() - t1) / 1e9;
                });
                consumer.start();
                consumer.join();

                if (run > 0)
                {
                    System.out.printf("RESULT lang=java mode=burst size=%d run=%d msgs=%d publish_msgs_per_sec=%.0f drain_msgs_per_sec=%.0f publish_minflt=%d%n",
                        size, run, n, n / pubSecs, n / drainSecs[0], pubFaults);
                }
            }
        }
    }

    /** Closed-loop ping-pong, one message outstanding; round-trip time in ns. */
    static void latency(final Aeron aeron, final int size, final int count, final int warmup) throws Exception
    {
        try (Publication ping = aeron.addPublication(CHANNEL, PING);
            Subscription pingSub = aeron.addSubscription(CHANNEL, PING);
            Publication pong = aeron.addPublication(CHANNEL, PONG);
            Subscription pongSub = aeron.addSubscription(CHANNEL, PONG))
        {
            await(ping);
            await(pong);
            final UnsafeBuffer src = new UnsafeBuffer(ByteBuffer.allocateDirect(size));
            final UnsafeBuffer echoBuf = new UnsafeBuffer(ByteBuffer.allocateDirect(size));
            stop = false;

            final FragmentAssembler echo = new FragmentAssembler((b, o, l, h) ->
            {
                echoBuf.putBytes(0, b, o, l);
                offer(pong, echoBuf, l);
            });
            final Thread ponger = new Thread(() ->
            {
                pinThread(pinConsumer);
                while (!stop)
                {
                    pingSub.poll(echo, FRAGMENT_LIMIT);
                }
            });
            ponger.start();
            pinThread(pinProducer);

            final long flt0 = minorFaults();
            final int[] got = new int[1];
            final FragmentAssembler reply = new FragmentAssembler((b, o, l, h) -> got[0]++);
            final long[] samples = new long[count];
            for (int i = 0; i < warmup + count; i++)
            {
                got[0] = 0;
                final long t0 = System.nanoTime();
                offer(ping, src, size);
                while (got[0] == 0)
                {
                    pongSub.poll(reply, FRAGMENT_LIMIT);
                }
                final long rtt = System.nanoTime() - t0;
                if (i >= warmup)
                {
                    samples[i - warmup] = rtt;
                }
            }
            stop = true;
            ponger.join();

            final long faults = minorFaults() - flt0;
            Arrays.sort(samples);
            System.out.printf("RESULT lang=java mode=latency size=%d samples=%d p50_ns=%d p90_ns=%d p99_ns=%d p999_ns=%d max_ns=%d minflt=%d%n",
                size, count, pct(samples, 0.50), pct(samples, 0.90), pct(samples, 0.99), pct(samples, 0.999), samples[count - 1], faults);
        }
    }

    /** Minor page faults of this process so far (field 10 of /proc/self/stat). */
    static long minorFaults()
    {
        try
        {
            final String stat = new String(Files.readAllBytes(Paths.get("/proc/self/stat")));
            final String[] f = stat.substring(stat.lastIndexOf(')') + 2).split(" ");
            return Long.parseLong(f[7]); // fields after the command name: state is f[0], minflt is f[7]
        }
        catch (final Exception e)
        {
            return -1;
        }
    }

    static long pct(final long[] sorted, final double p)
    {
        return sorted[(int)Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)];
    }
}
