package cp1.tests.helper;

import cp1.base.*;
import cp1.solution.TransactionManagerFactory;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Helper {

    private final ArrayList<Resource> resources = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final List<SyncPoint> syncPoints = new ArrayList<>();
    private final BaseResourceControl resourceControl;
    private int threadNo = 0;
    private TransactionManager tm;
    private Throwable failedWith;

    public Helper(int simpleResources, boolean strictChecking) {
        for (int i = 0; i < simpleResources; i++) {
            resources.add(new SimpleTestResource(new SimpleResourceId(i)));
        }
        resourceControl = strictChecking ? new StrictResourceControl() : new BaseResourceControl();
    }

    public Helper(int simpleResources) {
        this(simpleResources, true);
    }

    public SyncPoint newSyncPoint() {
        SyncPoint ret = new SyncPoint(0);
        syncPoints.add(ret);
        return ret;
    }

    public Resource getResource(int id) {
        return resources.get(id);
    }

    public ResourceId getResourceId(int id) {
        return resources.get(id).getId();
    }

    public TransactionManager getTransactionManager() {
        return tm;
    }

    public void thread(ThreadCb fn) {
        int no = threadNo++;
        threads.add(new Thread(() -> {
            try {
                fn.run(no);
            } catch (Exception | AssertionFailedError e) {
                synchronized (this) {
                    if (failedWith == null) {
                        System.err.println("Thread " + Thread.currentThread().getName() + " failed the test");
                        e.printStackTrace();
                        failedWith = e;
                    }
                    for (Thread t : threads) {
                        if (Thread.currentThread() != t)
                            t.interrupt();
                    }
                }
            }
        }, "Test-" + no));
        if (threads.size() >= 2) {
            if (threads.get(threads.size() - 2).getId() >= threads.get(threads.size() - 1).getId())
                throw new RuntimeException("Thread Ids should be incrementing; this will cause issues with tests, so aborting early");
        }
    }

    public void run(LocalTimeProvider provider) throws InterruptedException {
        tm = TransactionManagerFactory.newTM(resources, provider);
        for (SyncPoint p : syncPoints)
            p.latch = new CountDownLatch(threads.size());
        for (Thread t : threads)
            t.start();
        for (Thread t : threads)
            t.join();
        if (failedWith != null)
            throw new RuntimeException("Test failed due to a worker thread", failedWith);
    }

    public void transaction(TransactionCb r, boolean expectAbort) {
        try {
            tm.startTransaction();
        } catch (AnotherTransactionActiveException e) {
            throw new RuntimeException("Failed to start transaction", e);
        }

        TransactionWrapper wrapper = new TransactionWrapper();
        try {
            r.run(wrapper);

            if (wrapper.rolledBack) {
                if (tm.isTransactionActive() || tm.isTransactionAborted())
                    throw new RuntimeException("Transaction active or aborted even though we rolled back");
                return;
            }
        } catch (Exception e) {
            if (!expectAbort || !(e instanceof ActiveTransactionAborted)) {
                resourceControl.onResourcesReleased();
                tm.rollbackCurrentTransaction();
                throw new RuntimeException("Transaction failed", e);
            }
        }

        resourceControl.onResourcesReleased();

        if (expectAbort) {
            if (!tm.isTransactionAborted())
                throw new RuntimeException("Transaction was supposed to get aborted, but it wasn't");

            tm.rollbackCurrentTransaction();
        } else {
            if (tm.isTransactionAborted())
                throw new RuntimeException("Transaction was not supposed to get aborted, but it was");

            try {
                tm.commitCurrentTransaction();
            } catch (NoActiveTransactionException | ActiveTransactionAborted e) {
                throw new RuntimeException("Failed to commit transaction", e);
            }
        }
    }

    public void transaction(TransactionCb r) {
        transaction(r, false);
    }

    public void transactionExpectAbort(TransactionCb r) {
        transaction(r, true);
    }




    @SuppressWarnings("unchecked")
    public static <T extends Resource> ResourceOperation makeOperation(OperationExecuteCb<T> exec, OperationUndoCb<T> undo) {
        return new ResourceOperation() {
            @Override
            public void execute(Resource r) throws ResourceOperationException {
                exec.execute(this, (T) r);
            }

            @Override
            public void undo(Resource r) {
                undo.undo(this, (T) r);
            }
        };
    }

    public class TransactionWrapper {

        private boolean rolledBack;

        public void execute(ResourceId rid, ResourceOperation op) throws ActiveTransactionAborted, ResourceOperationException, UnknownResourceIdException, NoActiveTransactionException, InterruptedException {
            getTransactionManager().operateOnResourceInCurrentTransaction(rid, new ResourceOperation() {
                @Override
                public void execute(Resource r) throws ResourceOperationException {
                    resourceControl.onResourceAcquired(r);
                    op.execute(r);
                }

                @Override
                public void undo(Resource r) {
                    op.undo(r);
                }
            });
        }

        public void rollback() {
            rolledBack = true;
            resourceControl.onResourcesReleased();
            getTransactionManager().rollbackCurrentTransaction();
        }

    }

    public interface ThreadCb {
        void run(int index) throws Exception;
    }

    public interface TransactionCb {
        void run(TransactionWrapper t) throws Exception;
    }

    public interface OperationExecuteCb<T extends Resource> {
        void execute(ResourceOperation op, T r) throws ResourceOperationException;
    }

    public interface OperationUndoCb<T extends Resource> {
        void undo(ResourceOperation op, T r);
    }

}
