import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Dz4 {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    interface Shutdownable {
        void shutdown();
    }

    public interface ShutdownableExecutor extends Shutdownable, Executor {}

    static class ThreadPerTaskExecutor implements ShutdownableExecutor {
        private final List<Thread> pool = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void execute(Runnable r) {
            Thread t = new Thread(r);
            pool.add(t);
            t.start();
        }

        @Override
        public void shutdown() {
            for (Thread t : pool) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {
        protected static final Runnable EXIT_TASK = () -> {};
        private final AtomicInteger seq = new AtomicInteger(0);
        protected final List<Thread> workers;
        protected final List<BlockingDeque<Runnable>> queues;
        private volatile boolean active = true;

        RoundRobinExecutor(int size) {
            this.queues = new ArrayList<>(size);
            this.workers = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                queues.add(new LinkedBlockingDeque<>());
            }
            for (int i = 0; i < size; i++) {
                workers.add(makeWorker(i));
            }
            for (Thread t : workers) {
                t.start();
            }
        }

        Thread makeWorker(int idx) {
            return new Thread(() -> {
                while (true) {
                    Runnable r;
                    try {
                        r = queues.get(idx).takeFirst();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (r == EXIT_TASK) {
                        return;
                    }
                    r.run();
                }
            });
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (active) {
                    active = false;
                    for (BlockingDeque<Runnable> q : queues) {
                        q.addLast(EXIT_TASK);
                    }
                }
            }
            for (Thread t : workers) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public synchronized void execute(Runnable r) {
            if (!active) {
                throw new RejectedExecutionException();
            }
            int idx = Math.abs(seq.incrementAndGet()) % queues.size();
            queues.get(idx).addLast(r);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {
        WorkStealingExecutor(int size) {
            super(size);
        }

        @Override
        Thread makeWorker(int idx) {
            return new Thread(() -> {
                while (true) {
                    Runnable r = queues.get(idx).pollFirst();
                    if (r == null) {
                        r = trySteal(idx);
                    }
                    if (r == null) {
                        try {
                            r = queues.get(idx).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (r == null) {
                            continue;
                        }
                    }
                    if (r == EXIT_TASK) {
                        r = trySteal(idx);
                        if (r == null) {
                            return;
                        }
                        queues.get(idx).addLast(EXIT_TASK);
                    }
                    r.run();
                }
            });
        }

        private Runnable trySteal(int id) {
            int total = queues.size();
            for (int i = 1; i < total; i++) {
                int target = (id + i) % total;
                BlockingDeque<Runnable> q = queues.get(target);
                Runnable r = q.pollLast();
                if (r == EXIT_TASK) {
                    r = q.pollLast();
                    q.addLast(EXIT_TASK);
                }
                if (r != null) {
                    return r;
                }
            }
            return null;
        }
    }

    static class FixedThreadPoolExecutor implements ShutdownableExecutor {
        private final ExecutorService srv;

        FixedThreadPoolExecutor(int size) {
            this.srv = Executors.newFixedThreadPool(size);
        }

        @Override
        public void execute(Runnable r) {
            srv.execute(r);
        }

        @Override
        public void shutdown() {
            srv.shutdown();
            try {
                srv.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static volatile long sink;

    static void blackHole(long difficulty) {
        long res = 0;
        for (long i = 0; i < difficulty; i++) {
            res = (res + i) ^ (res >>> 3);
        }
        sink = res;
    }

    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000;

    static final int MEAN_TASK_TIME = (int) Math.round((double) TARGET_OPTIMAL_FULL_TIME / TASK_NUMBER * THREAD_NUMBER);
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2 + 1;
    static final int ESTIMATED_OPTIMAL_TIME = MEAN_TASK_TIME * TASK_NUMBER / THREAD_NUMBER;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;

    static Runnable createTask(int duration) {
        return () -> blackHole(duration * 1_000_000L);
    }

    static int createTaskDuration(TaskDistribution dist, int id, Random rng) {
        switch (dist) {
            case UNIFORM:
                return rng.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);
            case PERIODIC:
                return id % THREAD_NUMBER == 0 ? MEAN_TASK_TIME * THREAD_NUMBER : 0;
            case PARETO:
                double scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1.0) / PARETO_SHAPE;
                double val = scale / Math.pow(1.0 - rng.nextDouble(), 1.0 / PARETO_SHAPE);
                return (int) Math.min(Math.round(val), PARETO_RAW_MAX_TASK_TIME);
            default:
                return 0;
        }
    }

    static int[] createTaskDurations(TaskDistribution dist) {
        Random rng = new Random(RANDOM_SEED);
        int[] arr = new int[TASK_NUMBER];
        long total = 0;
        for (int i = 0; i < TASK_NUMBER; i++) {
            arr[i] = createTaskDuration(dist, i, rng);
            total += arr[i];
        }
        double factor = (double) TARGET_TOTAL_TASK_TIME / total;
        double rem = 0;
        long norm = 0;
        for (int i = 0; i < TASK_NUMBER; i++) {
            double calc = arr[i] * factor + rem;
            arr[i] = (int) calc;
            rem = calc - arr[i];
            norm += arr[i];
        }
        int idx = 0;
        while (norm < TARGET_TOTAL_TASK_TIME) {
            arr[idx % TASK_NUMBER]++;
            norm++;
            idx++;
        }
        idx = 0;
        while (norm > TARGET_TOTAL_TASK_TIME) {
            int targetIdx = idx % TASK_NUMBER;
            if (arr[targetIdx] > 0) {
                arr[targetIdx]--;
                norm--;
            }
            idx++;
        }
        return arr;
    }

    static List<Runnable> createTasks(TaskDistribution dist) {
        int[] times = createTaskDurations(dist);
        List<Runnable> list = new ArrayList<>(TASK_NUMBER);
        for (int t : times) {
            list.add(createTask(t));
        }
        return list;
    }

    public static class MetricPair {
        public final long first;
        public final long second;

        public MetricPair(long first, long second) {
            this.first = first;
            this.second = second;
        }
    }

    public static MetricPair measureExecutor(ShutdownableExecutor exec, TaskDistribution dist) {
        List<Runnable> jobs = createTasks(dist);
        long t0 = System.nanoTime();
        for (Runnable r : jobs) {
            exec.execute(r);
        }
        long t1 = System.nanoTime();
        exec.shutdown();
        long t2 = System.nanoTime();
        return new MetricPair(t1 - t0, t2 - t0);
    }

    public static void main(String[] args) {
        System.out.println("Target optimal time: " + TARGET_OPTIMAL_FULL_TIME);
        System.out.println("Estimated optimal time: " + ESTIMATED_OPTIMAL_TIME);
        MetricPair r0 = measureExecutor(new ThreadPerTaskExecutor(), TaskDistribution.UNIFORM);
        System.out.println("Thread per task executor submtition time: " + r0.first / 1000000.0);
        System.out.println("Thread per task execution time: " + r0.second / 1000000.0);
        for (TaskDistribution d : TaskDistribution.values()) {
            System.out.println("Task distribution: " + d);
            MetricPair r1 = measureExecutor(new RoundRobinExecutor(THREAD_NUMBER), d);
            System.out.println("Round Robin Executor submition time: " + r1.first / 1000000.0);
            System.out.println("Round Robin Executor execution time: " + r1.second / 1000000.0);
            MetricPair r2 = measureExecutor(new WorkStealingExecutor(THREAD_NUMBER), d);
            System.out.println("Work Stealing Executor submition time: " + r2.first / 1000000.0);
            System.out.println("Work Stealing Executor execution time: " + r2.second / 1000000.0);
            MetricPair r3 = measureExecutor(new FixedThreadPoolExecutor(THREAD_NUMBER), d);
            System.out.println("Fixed Thread Pool submition time: " + r3.first / 1000000.0);
            System.out.println("Fixed Thread Pool execution time: " + r3.second / 1000000.0);
        }
    }
}



