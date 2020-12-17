package cp1.tests;

import cp1.base.*;
import cp1.tests.helper.CustomTimeProvider;
import cp1.tests.helper.Helper;
import cp1.tests.helper.SimpleTestResource;
import cp1.tests.helper.SyncPoint;
import cp1.solution.TransactionManagerFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testy do pierwszego zadania z Programowania Współbieżnego z roku 2020. Prosiłbym o nie wysyłanie ich z własnym
 * projektem na Moodle ze względu na testy antyplagiatowe które być może obejmują testy.
 *
 * Kod jest oczwiście niskiej jakości, ale powinien wyłapać niektóre częste błedy.
 *
 * Autor    :  Paweł Pawłowski
 * Licencja :  MIT
 */
public class Transaction6 {

    public static LocalTimeProvider ZeroTimeProvider = () -> 0;
    public static ResourceOperation NopOperation = Helper.makeOperation((o, r) -> {}, (o, r) -> {});
    public static ResourceOperation FailOperation = Helper.makeOperation((o, r) -> { throw new ResourceOperationException(r.getId(), o); }, (o, r) -> { throw new RuntimeException("Can't revert this operation, never"); });

    class TrackedOperation extends ResourceOperation {

        @Override
        public void execute(Resource r) {
            if (!((SimpleTestResource) r).activeOperations.add(this))
                throw new RuntimeException("A tracked operation can be only ran before a revert");
        }

        @Override
        public void undo(Resource r) {
            if (!((SimpleTestResource) r).activeOperations.remove(this))
                throw new RuntimeException("Failed to revert a tracked operation (not found)");
        }

        public void check(Resource r) {
            assertTrue(((SimpleTestResource) r).activeOperations.contains(this));
        }

        public void checkMissing(Resource r) {
            assertFalse(((SimpleTestResource) r).activeOperations.contains(this));
        }

    }

    @Test
    public void statusOkayWhenNoTransactionActive() {
        TransactionManager t = TransactionManagerFactory.newTM(Collections.emptyList(), ZeroTimeProvider);
        assertFalse(t.isTransactionActive());
        assertFalse(t.isTransactionAborted());
    }

    @Test
    public void statusOkayWhenTransactionActive() throws AnotherTransactionActiveException {
        TransactionManager t = TransactionManagerFactory.newTM(Collections.emptyList(), ZeroTimeProvider);
        t.startTransaction();
        assertTrue(t.isTransactionActive());
        assertFalse(t.isTransactionAborted());
        t.rollbackCurrentTransaction();
    }

    @Test
    public void statusOkayWhenTransactionComitted() throws AnotherTransactionActiveException, NoActiveTransactionException, ActiveTransactionAborted {
        TransactionManager t = TransactionManagerFactory.newTM(Collections.emptyList(), ZeroTimeProvider);
        t.startTransaction();
        t.commitCurrentTransaction();
        assertFalse(t.isTransactionActive());
        assertFalse(t.isTransactionAborted());
    }

    @Test
    public void statusOkayWhenTransactionRolledBack() throws AnotherTransactionActiveException {
        TransactionManager t = TransactionManagerFactory.newTM(Collections.emptyList(), ZeroTimeProvider);
        t.startTransaction();
        t.rollbackCurrentTransaction();
        assertFalse(t.isTransactionActive());
        assertFalse(t.isTransactionAborted());
        t.rollbackCurrentTransaction();
    }

    @Test
    public void oneTransactionAtATime() throws InterruptedException {
        Helper helper = new Helper(1);
        helper.thread(i -> {
            helper.getTransactionManager().startTransaction();
            assertThrows(AnotherTransactionActiveException.class, () -> helper.getTransactionManager().startTransaction());
        });
        helper.run(ZeroTimeProvider);
    }

    @Test
    public void changesSave() throws InterruptedException {
        TrackedOperation op1 = new TrackedOperation();
        Helper helper = new Helper(1);
        helper.thread(i -> helper.transaction(t -> {
            t.execute(helper.getResourceId(0), op1);
        }));
        helper.run(ZeroTimeProvider);
        op1.check(helper.getResource(0));
    }

    @Test
    public void completionUnlocksResource() throws InterruptedException {
        Helper helper = new Helper(1);
        CountDownLatch latch = new CountDownLatch(1);
        helper.thread(i -> {
            helper.transaction(t -> t.execute(helper.getResourceId(0), NopOperation));
            latch.countDown();
        });
        helper.thread(i -> helper.transaction(t -> {
            latch.await();
            t.execute(helper.getResourceId(0), NopOperation);
            // if we got here that's cool!
        }));
        helper.run(ZeroTimeProvider);
    }

    @Test
    public void waitingForResourceWorks() throws InterruptedException {
        Helper helper = new Helper(1);
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        helper.thread(i -> {
            helper.transaction(t -> t.execute(helper.getResourceId(0), NopOperation));
            latch.countDown();
            latch2.await();
            Thread.sleep(1); // sadly we do not have any way to check whether the thread is always waiting. let's wait 1ms
        });
        helper.thread(i -> helper.transaction(t -> {
            latch.await();
            latch2.countDown();
            t.execute(helper.getResourceId(0), NopOperation);
            // if we got here that's cool!
        }));
        helper.run(ZeroTimeProvider);
    }

    @Test
    public void simple2ThreadDeadlockResolutionPlusExtra() throws InterruptedException {
        Helper helper = new Helper(2);
        SyncPoint syncPoint = helper.newSyncPoint();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean tooFast = new AtomicBoolean(true);
        helper.thread(i -> helper.transaction(t -> {
            t.execute(helper.getResourceId(0), NopOperation);
            syncPoint.arrived();
            t.execute(helper.getResourceId(1), NopOperation);
            latch.await();
        }));
        helper.thread(i -> {
            helper.transactionExpectAbort(t -> {
                t.execute(helper.getResourceId(1), NopOperation);
                syncPoint.arrived();
                Thread.sleep(10);
                try {
                    t.execute(helper.getResourceId(0), NopOperation);
                } catch (ActiveTransactionAborted e) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ig) {
                        throw new IllegalStateException("Post-operation sleep should not be interrupted, if it does it probably means you self-interrupted the thread. Ensure you are not interrupting the thread if it is the same as the one you are trying to get access from.");
                    }
                    tooFast.set(false);
                    throw e;
                }
            });
            latch.countDown();
        });
        helper.run(ZeroTimeProvider);
    }

    /**
     * Simple test where we create a resource access loop, 1-st thread depends on 2-nd, 2-nd on 3-rd, etc., then
     * n-th depends on 1-st. We also check ordering based on thread id (all time values are zero).
     * @param n number of threads/resources
     */
    private void simpleNThreadDeadlockResolution(int n) throws InterruptedException {
        Lock lock = new ReentrantLock();
        int[] expectedNum = new int[] { n - 2 }; // has to be boxed in some way so we can write to it in the lambda
        Helper helper = new Helper(n);
        SyncPoint syncPoint = helper.newSyncPoint();
        for (int c = 0; c < n; c++) {
            helper.thread(i -> helper.transaction(t -> {
                t.execute(helper.getResourceId(i), NopOperation);
                syncPoint.arrived();
                t.execute(helper.getResourceId((i + 1) % n), NopOperation);
                if (!lock.tryLock())
                    throw new RuntimeException("Data race");
                if (expectedNum[0] != i)
                    throw new RuntimeException("Wrong threaded waked up");
                expectedNum[0] = i - 1;
                Thread.sleep(1); // add a sleep to ensure no races
                lock.unlock();
            }, i == n - 1));
        }
        helper.run(ZeroTimeProvider);
    }

    @Test
    public void simple3ThreadDeadlockResolution() throws InterruptedException {
        simpleNThreadDeadlockResolution(3);
    }

    @Test
    public void simple5ThreadDeadlockResolution() throws InterruptedException {
        simpleNThreadDeadlockResolution(5);
    }

    @Test
    public void simple100ThreadDeadlockResolution() throws InterruptedException {
        simpleNThreadDeadlockResolution(100);
    }

    private void doCheckTimeOrdering(long time1, long time2) throws InterruptedException {
        Helper helper = new Helper(2);
        SyncPoint syncPoint = helper.newSyncPoint();
        for (int c = 0; c < 2; c++) {
            helper.thread(i -> {
                CustomTimeProvider.set(i == 1 ? time2 : time1);
                helper.transaction(t -> {
                    t.execute(helper.getResourceId(i), NopOperation);
                    syncPoint.arrived();
                    t.execute(helper.getResourceId((i + 1) % 2), NopOperation);

                }, i == (time1 <= time2 ? 1 : 0));
            });
        }
        helper.run(CustomTimeProvider.instance);
    }

    /**
     * Check edge cases when handling time values for cancellation.
     */
    @Test
    public void checkTimeOrdering() throws InterruptedException {
        doCheckTimeOrdering(1, 2);
        doCheckTimeOrdering(2, 1);
        doCheckTimeOrdering(Long.MIN_VALUE, Long.MIN_VALUE + 1);
        doCheckTimeOrdering(Long.MIN_VALUE + 1, Long.MIN_VALUE);
        doCheckTimeOrdering(Long.MAX_VALUE, Long.MAX_VALUE - 1);
        doCheckTimeOrdering(Long.MAX_VALUE - 1, Long.MAX_VALUE);
        doCheckTimeOrdering(Long.MIN_VALUE, Long.MAX_VALUE);
        doCheckTimeOrdering(Long.MAX_VALUE, Long.MIN_VALUE);
    }

    /**
     * This test checks the following:
     * - failed operations never are reverted
     * - a failed operation does not cause a revert
     * - we can still perform other operations after a failed operation
     */
    @Test
    public void failDoesNotRevertOrAbort() throws InterruptedException {
        Helper helper = new Helper(1);
        helper.thread(i -> helper.transaction(t -> {
            TrackedOperation op1 = new TrackedOperation();
            TrackedOperation op2 = new TrackedOperation();
            TrackedOperation op3 = new TrackedOperation();
            t.execute(helper.getResourceId(0), op1);
            t.execute(helper.getResourceId(0), op2);
            assertThrows(ResourceOperationException.class, () -> t.execute(helper.getResourceId(0), FailOperation));
            op1.check(helper.getResource(0));
            op2.check(helper.getResource(0));
            t.execute(helper.getResourceId(0), op3);
            op3.check(helper.getResource(0));
        }));
        helper.run(ZeroTimeProvider);
    }

    /**
     * Even if a transaction fails, it still must keep the resource locked. Based on question on the forum:
     * https://moodle.mimuw.edu.pl/mod/forum/discuss.php?d=3922
     */
    @Test
    public void failKeepsLock() throws InterruptedException {
        Helper helper = new Helper(1);
        CountDownLatch latch = new CountDownLatch(1);
        helper.thread(i -> {
            helper.transaction(t -> {
                latch.countDown();
                assertThrows(ResourceOperationException.class, () -> t.execute(helper.getResourceId(0), FailOperation));
                Thread.sleep(10);
            });
        });
        helper.thread(i -> helper.transaction(t -> {
            latch.await();
            t.execute(helper.getResourceId(0), NopOperation);
            // if we got here w/o an error that's cool! StrictResourceControl will enforce that we do not get here
            // before  the first transaction exists
        }));
        helper.run(ZeroTimeProvider);
    }

    @Test
    public void rollbackAfterFailReleasesResources() throws InterruptedException {
        Helper helper = new Helper(1);
        SyncPoint syncPoint = helper.newSyncPoint();
        helper.thread(i -> {
            helper.transaction(t -> {
                assertThrows(ResourceOperationException.class, () -> t.execute(helper.getResourceId(0), FailOperation));
            });
            syncPoint.arrived();
        });
        helper.thread(i -> helper.transaction(t -> {
            syncPoint.arrived();
            t.execute(helper.getResourceId(0), NopOperation);
        }));
        helper.run(ZeroTimeProvider);
    }

    @Test
    public void rollBackWorks() throws InterruptedException {
        TrackedOperation op1 = new TrackedOperation();
        Helper helper = new Helper(1);
        helper.thread(i -> {
            helper.transaction(t -> {
                t.execute(helper.getResourceId(0), op1);
                op1.check(helper.getResource(0));
                t.rollback();
                op1.checkMissing(helper.getResource(0));
            });
            op1.checkMissing(helper.getResource(0));
        });
        helper.run(ZeroTimeProvider);
        op1.checkMissing(helper.getResource(0));
    }

    @Test
    public void rollBackViaAbortWorks() throws InterruptedException {
        TrackedOperation op1 = new TrackedOperation();
        Helper helper = new Helper(2);
        SyncPoint syncPoint = helper.newSyncPoint();
        helper.thread(i -> helper.transaction(t -> {
            t.execute(helper.getResourceId(0), NopOperation);
            syncPoint.arrived();
            t.execute(helper.getResourceId(1), NopOperation);
        }));
        helper.thread(i -> helper.transactionExpectAbort(t -> {
            t.execute(helper.getResourceId(1), op1);
            syncPoint.arrived();
            try {
                t.execute(helper.getResourceId(0), NopOperation);
            } catch (ActiveTransactionAborted a) {
                op1.check(helper.getResource(1)); // still should be there, as rollback will be done when we exit by Helper
                throw a;
            }
        }));
        helper.run(ZeroTimeProvider);
        helper.transaction(t -> t.execute(helper.getResourceId(0), NopOperation));
        op1.checkMissing(helper.getResource(1));
    }

    @Test
    public void delayedAbortReactionDoesNotLockUpDeadlockResolution() throws InterruptedException {
        Helper helper = new Helper(2);
        SyncPoint syncPoint = new SyncPoint(2);
        SyncPoint syncPoint2 = new SyncPoint(2);
        helper.thread(i -> helper.transaction(t -> {
            t.execute(helper.getResourceId(0), NopOperation);
            syncPoint.arrived();
            t.execute(helper.getResourceId(1), NopOperation);
        }));
        helper.thread(i -> helper.transactionExpectAbort(t -> {
            t.execute(helper.getResourceId(1), NopOperation);
            syncPoint.arrived();
            try {
                t.execute(helper.getResourceId(0), NopOperation);
            } catch (ActiveTransactionAborted a) {
                syncPoint2.arrived();
                Thread.sleep(10L);
                throw a;
            }
        }));
        helper.thread(i -> helper.transaction(t -> {
            syncPoint2.arrived();
            t.execute(helper.getResourceId(0), NopOperation);
        }));
        helper.run(ZeroTimeProvider);
    }

    @Test
    public void orderOfRevertIsCorrect() throws InterruptedException {
        Helper helper = new Helper(2);
        helper.thread(i -> helper.transaction(t -> {
            int[] x = new int[] { 0 };
            t.execute(helper.getResourceId(0), Helper.makeOperation((a, b) -> {}, (a, b) -> assertEquals(x[0]++, 3)));
            t.execute(helper.getResourceId(1), Helper.makeOperation((a, b) -> {}, (a, b) -> assertEquals(x[0]++, 2)));
            t.execute(helper.getResourceId(1), Helper.makeOperation((a, b) -> {}, (a, b) -> assertEquals(x[0]++, 1)));
            t.execute(helper.getResourceId(0), Helper.makeOperation((a, b) -> {}, (a, b) -> assertEquals(x[0]++, 0)));
            t.rollback();
        }));
        helper.run(ZeroTimeProvider);
    }

}
