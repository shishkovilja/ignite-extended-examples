package org.apache.ignite;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.TransactionIsolationTest.Operation.GET;
import static org.apache.ignite.TransactionIsolationTest.Operation.PUT;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

/**
 *
 */
@SuppressWarnings("resource")
@RunWith(Parameterized.class)
public class TransactionIsolationTest extends GridCommonAbstractTest {
    @Parameter(0)
    public Operation tx1Operation;

    @Parameter(1)
    public TransactionConcurrency tx1Concurrency;

    @Parameter(2)
    public TransactionIsolation tx1Isolation;

    @Parameter(3)
    public Operation tx2Operation;

    @Parameter(4)
    public TransactionConcurrency tx2Concurrency;

    @Parameter(5)
    public TransactionIsolation tx2Isolation;

    private IgniteEx ign;

    /** Cache. */
    private IgniteCache<Integer, String> cache;

    private CountDownLatch txLatch;


    /**
     *
     */
    @Parameters(name = "TX1 [{0},{1},{2}], TX2 [{3},{4},{5}")
    public static Iterable<Object[]> params() {
        return List.of(
            new Object[]{GET, PESSIMISTIC, READ_COMMITTED, GET, PESSIMISTIC, READ_COMMITTED},
            new Object[]{GET, PESSIMISTIC, READ_COMMITTED, PUT, PESSIMISTIC, READ_COMMITTED},
            new Object[]{PUT, PESSIMISTIC, READ_COMMITTED, GET, PESSIMISTIC, READ_COMMITTED},
            new Object[]{PUT, PESSIMISTIC, READ_COMMITTED, PUT, PESSIMISTIC, READ_COMMITTED},

            new Object[]{GET, PESSIMISTIC, REPEATABLE_READ, GET, PESSIMISTIC, REPEATABLE_READ},
            new Object[]{GET, PESSIMISTIC, REPEATABLE_READ, PUT, PESSIMISTIC, REPEATABLE_READ},
            new Object[]{PUT, PESSIMISTIC, REPEATABLE_READ, GET, PESSIMISTIC, REPEATABLE_READ},
            new Object[]{PUT, PESSIMISTIC, REPEATABLE_READ, PUT, PESSIMISTIC, REPEATABLE_READ},

            new Object[]{GET, PESSIMISTIC, REPEATABLE_READ, GET, PESSIMISTIC, READ_COMMITTED},
            new Object[]{GET, PESSIMISTIC, REPEATABLE_READ, PUT, PESSIMISTIC, READ_COMMITTED},
            new Object[]{PUT, PESSIMISTIC, REPEATABLE_READ, GET, PESSIMISTIC, READ_COMMITTED},
            new Object[]{PUT, PESSIMISTIC, REPEATABLE_READ, PUT, PESSIMISTIC, READ_COMMITTED},

            new Object[]{GET, PESSIMISTIC, READ_COMMITTED, GET, PESSIMISTIC, REPEATABLE_READ},
            new Object[]{GET, PESSIMISTIC, READ_COMMITTED, PUT, PESSIMISTIC, REPEATABLE_READ},
            new Object[]{PUT, PESSIMISTIC, READ_COMMITTED, GET, PESSIMISTIC, REPEATABLE_READ},
            new Object[]{PUT, PESSIMISTIC, READ_COMMITTED, PUT, PESSIMISTIC, REPEATABLE_READ}
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(new NullLogger())
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ign = startGrid();
        cache = ign.cache(DEFAULT_CACHE_NAME);
        txLatch = new CountDownLatch(1);

        IntStream.range(0, 3)
            .forEach(i -> cache.put(i, "INITIAL_VALUE"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        ign = null;
        cache = null;
        txLatch = null;
    }

    /**
     *
     */
    @Test
    public void test() throws IgniteCheckedException {
        var tx1Fut = runAsync(txRunnable(tx1Concurrency, tx1Isolation, tx1Operation, true));
        var tx2Fut = runAsync(txRunnable(tx2Concurrency, tx2Isolation, tx2Operation, false));

        tx1Fut.get();
        tx2Fut.get();

        System.err.println(">>>>>> Results: " + cache.getAll(Set.of(0, 1, 2)));

        doSleep(5000);
    }

    private RunnableX txRunnable(TransactionConcurrency concurrency, TransactionIsolation isolation,
        Operation op, boolean firstTx) {
        return () -> {
            String txName = firstTx ? "TX-1" : "TX-2";

            Consumer<Integer> cacheAction = cacheAction(op, firstTx, txName);

            try (Transaction tx = ign.transactions().txStart(concurrency, isolation)) {
                String txInfo = '[' + txName + ',' + concurrency + ',' + isolation + ',' + op + ']';

                System.err.println(txInfo + " Started");

                int start = 0;

                if (firstTx) {
                    cacheAction.accept(0);

                    start = 1;

                    txLatch.countDown();
                }
                else {
                    System.err.println(txName + " Waiting for first tx");

                    txLatch.await();
                }

                for (int i = start; i < 3; i++)
                    cacheAction.accept(i);

                System.err.println(txName + " Before commit");
                tx.commit();
                System.err.println(txName + " After commit");
            }
        };
    }

    private Consumer<Integer> cacheAction(Operation op, boolean firstTx, String txName) {
        switch (op) {
            case GET:
                return i -> {
                    System.err.println(txName + " Got: [key=" + i + ", value=" + cache.get(i) + ']');

                    if (firstTx)
                        doSleep(5000);
                };

            case PUT:
                return i -> {
                    cache.put(i, txName);

                    System.err.println(txName + " Put: [key=" + i + ", value=" + txName + ']');

                    if (firstTx)
                        doSleep(5000);
                };

            default:
                throw new IllegalArgumentException();
        }
    }

    enum Operation {
        GET, PUT;
    }
}
