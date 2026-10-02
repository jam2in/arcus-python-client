import java.io.*;
import java.nio.charset.StandardCharsets;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import net.spy.memcached.*;
import net.spy.memcached.transcoders.*;

/** Single-connection, closed-loop reference runner. One completed request per sample. */
public final class Benchmark {
  static final Transcoder<byte[]> CODEC = new Transcoder<byte[]>() {
    public CachedData encode(byte[] data) { return new CachedData(2048, data, getMaxSize()); }
    public byte[] decode(CachedData data) { return data.getData(); }
    public boolean asyncDecode(CachedData data) { return false; }
    public int getMaxSize() { return 1024 * 1024; }
  };
  static final int TTL = 300;
  static double lastElapsed, lastCpu;
  static List<long[]> measure(ArcusClient client, String operation, String[] keys,
      byte[] value, int concurrency, double seconds) throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(concurrency);
    CountDownLatch ready = new CountDownLatch(concurrency);
    CountDownLatch start = new CountDownLatch(1);
    long[] deadline = new long[1];
    List<Future<List<long[]>>> futures = new ArrayList<>();
    for (int worker = 0; worker < concurrency; worker++) {
      final int offset = worker;
      futures.add(workers.submit(() -> {
        List<long[]> samples = new ArrayList<>(); int index = offset;
        ready.countDown(); start.await();
        while (System.nanoTime() < deadline[0]) {
          String key = keys[(index++) % keys.length];
          long before = System.nanoTime(); int status = 0;
          try {
            if (operation.equals("set")) {
              if (!client.set(key, TTL, value, CODEC).get(5, TimeUnit.SECONDS)) status = 1;
            } else if (!Arrays.equals(value, client.asyncGet(key, CODEC).get(5, TimeUnit.SECONDS))) status = 2;
          } catch (TimeoutException error) { status = 3; }
          catch (Exception error) { status = 4; }
          samples.add(new long[] {System.nanoTime() - before, status});
        }
        return samples;
      }));
    }
    ready.await();
    long before = System.nanoTime();
    com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    long cpuBefore = os.getProcessCpuTime();
    deadline[0] = before + (long)(seconds * 1e9); start.countDown();
    List<long[]> all = new ArrayList<>();
    try { for (Future<List<long[]>> future : futures) all.addAll(future.get((long)seconds + 15, TimeUnit.SECONDS)); }
    finally { workers.shutdownNow(); }
    lastElapsed = (System.nanoTime() - before) / 1e9;
    lastCpu = (os.getProcessCpuTime() - cpuBefore) / 1e9;
    return all;
  }
  public static void main(String[] args) throws Exception {
    String operation = args[0], prefix = args[1], output = args[2];
    int size = Integer.parseInt(args[3]), concurrency = Integer.parseInt(args[4]);
    double duration = Double.parseDouble(args[5]), warmup = Double.parseDouble(args[6]);
    ConnectionFactoryBuilder factory = new ConnectionFactoryBuilder().setOpTimeout(5000).setUseNagleAlgorithm(true);
    ArcusClient client = ArcusClient.createArcusClient("zookeeper:2181", "python-benchmark", factory);
    String[] keys = new String[256]; byte[] value = new byte[size]; Arrays.fill(value, (byte)'x');
    try {
      for (int index = 0; index < keys.length; index++) {
        keys[index] = prefix + ":" + index;
        if (!client.set(keys[index], TTL, value, CODEC).get(5, TimeUnit.SECONDS)) throw new IllegalStateException("seed failed");
        if (!Arrays.equals(value, client.asyncGet(keys[index], CODEC).get(5, TimeUnit.SECONDS))) throw new IllegalStateException("seed mismatch");
      }
      measure(client, operation, keys, value, concurrency, warmup);
      List<long[]> samples = measure(client, operation, keys, value, concurrency, duration);
      try (PrintWriter writer = new PrintWriter(output, "UTF-8")) {
        writer.println("{\"elapsed_s\":" + lastElapsed + ",\"cpu_s\":" + lastCpu
          + ",\"java_version\":\"" + System.getProperty("java.version") + "\",\"samples\":[");
        for (int i = 0; i < samples.size(); i++) {
          long[] sample = samples.get(i);
          writer.print("[" + sample[0] + "," + sample[1] + "]" + (i + 1 == samples.size() ? "" : ","));
        }
        writer.println("]}");
      }
      for (String key : keys) client.delete(key).get(5, TimeUnit.SECONDS);
    } finally { client.shutdown(); }
  }
}
