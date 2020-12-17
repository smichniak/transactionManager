package cp1.solution;

import cp1.base.ActiveTransactionAborted;
import cp1.base.AnotherTransactionActiveException;
import cp1.base.LocalTimeProvider;
import cp1.base.NoActiveTransactionException;
import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;
import cp1.base.ResourceOperationException;
import cp1.base.TransactionManager;
import cp1.base.UnknownResourceIdException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class ImplementedTransactionManager implements TransactionManager {
    private static class SuccessfulOperation { // TODO To static or not to static?
        private ResourceId resourceId;
        private ResourceOperation operation;

        private SuccessfulOperation(ResourceId rid, ResourceOperation operation) {
            this.resourceId = rid;
            this.operation = operation;
        }

        private ResourceId getResourceId() {
            return resourceId;
        }

        private ResourceOperation getOperation() {
            return operation;
        }
    }

    private static class TransactionStartTime implements Comparable<TransactionStartTime> {
        private long startTime;
        private long threadId;

        private TransactionStartTime(long startTime, long threadId) {
            this.startTime = startTime;
            this.threadId = threadId;
        }

        private long getThreadId() {
            return threadId;
        }

        @Override
        public int compareTo(TransactionStartTime other) {
            if (other.startTime > startTime) { // Other older than this
                return 1;
            } else if (other.startTime < startTime) {
                return -1;
            } else {
                if (other.threadId > threadId) {
                    return 1;
                } else {
                    return -1;
                }
            }

        }
    }

    private LocalTimeProvider timeProvider;
    private Map<ResourceId, Resource> resources;
    private Map<ResourceId, Long> resourceLockedBy; // Key: ThreadId
    private Map<Long, ResourceId> waitsForResource; // Key: ThreadId, Value: Resource the thread is waiting for
    private Map<Long, TransactionStartTime> startTime; // Key: ThreadId, Value: Time of starting the transaction
    private Map<Long, Boolean> isAborted; // ThreadId as key
    private Semaphore resourceLockMutex = new Semaphore(1);

    private static ThreadLocal<Map<ImplementedTransactionManager, Deque<SuccessfulOperation>>> transactionOperations = ThreadLocal.withInitial(HashMap::new);


    public ImplementedTransactionManager(Collection<Resource> resources, LocalTimeProvider timeProvider) {
        this.resources = new ConcurrentHashMap<>(); // Concurrent or not?
        for (Resource resource : resources) {
            this.resources.put(resource.getId(), resource);
        }

        this.resourceLockedBy = new ConcurrentHashMap<>();
        this.waitsForResource = new ConcurrentHashMap<>();
        this.timeProvider = timeProvider;
        this.isAborted = new ConcurrentHashMap<>();
        this.startTime = new ConcurrentHashMap<>();
    }

    @Override
    public void startTransaction() throws AnotherTransactionActiveException {
        if (transactionOperations.get().containsKey(this)) {
            throw new AnotherTransactionActiveException();
        }
        long myThreadId = Thread.currentThread().getId();
        startTime.put(myThreadId, new TransactionStartTime(timeProvider.getTime(), myThreadId));
        transactionOperations.get().put(this, new ArrayDeque<>());
        isAborted.put(Thread.currentThread().getId(), false);

    }

    /**
     * Checks if a resource with a given Id is locked and tries
     * to lock it, if it isn't.
     *
     * @param rid Id of the resource we want to lock.
     * @return True if we locked the resource, false if we didn't.
     */
    private boolean lockResource(ResourceId rid) throws InterruptedException {
        resourceLockMutex.acquire();
        if (!resourceLockedBy.containsKey(rid)) {
            resourceLockedBy.put(rid, Thread.currentThread().getId());
            waitsForResource.remove(Thread.currentThread().getId());
            resourceLockMutex.release();
            return true;
        } else {
            resourceLockMutex.release();
            return false;
        }
    }

    private Collection<Long> findCycle() {
        List<Long> cycle = new ArrayList<>();
        boolean endOfPath = false;
        long start = Thread.currentThread().getId();
        cycle.add(start);
        ResourceId waitingFor = waitsForResource.get(start);
        long next = resourceLockedBy.get(waitingFor);

        while (!isAborted.get(next) && !endOfPath) {
            cycle.add(next);
//            System.err.println(cycle);
            if (start == next || !waitsForResource.containsKey(next)) {
                endOfPath = true;
            } else {
                ResourceId nextWaitingFor = waitsForResource.get(next);
                if (!resourceLockedBy.containsKey(nextWaitingFor)) {
                    endOfPath = true;
                } else {
                    next = resourceLockedBy.get(nextWaitingFor);

                }
            }

        }

        if (cycle.size() != 1 && cycle.get(0).equals(cycle.get(cycle.size() - 1))) {
            return cycle;
        } else {
            return new ArrayDeque<>(); // No cycle found
        }
    }

    private void abortYoungest(Collection<Long> cycle) {
        if (cycle.size() != 0) {
            List<TransactionStartTime> candidates = new ArrayList<>();
            for (long threadId : cycle) {
                candidates.add(startTime.get(threadId));
            }
            Collections.sort(candidates);
            long toAbort = candidates.get(0).getThreadId();

            isAborted.put(toAbort, true);
            for (Thread t : Thread.getAllStackTraces().keySet()) {
                if (t.getId() == toAbort) {
                    t.interrupt();
                    break;
                }
            }

        }

    }

    private synchronized void waitForResource(ResourceId rid) throws InterruptedException, ActiveTransactionAborted {
        long myThreadId = Thread.currentThread().getId();

        while (!lockResource(rid)) {
            if (!waitsForResource.containsKey(myThreadId)) {
                waitsForResource.put(myThreadId, rid);
                Collection<Long> cycle = findCycle();
                abortYoungest(cycle);
                notifyAll();
                if (isTransactionAborted()) {
                    waitsForResource.remove(myThreadId);
                    throw new ActiveTransactionAborted();
                } else if (Thread.currentThread().isInterrupted()) {
                    waitsForResource.remove(myThreadId);
                    throw new InterruptedException();
                }
            }
            try {
                wait();
            } catch (InterruptedException interrupted) {
                waitsForResource.remove(myThreadId);
                if (isTransactionAborted()) {
                    throw new ActiveTransactionAborted();
                } else {
                    throw interrupted;
                }
            }
        }

    }

    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation) throws
            NoActiveTransactionException, UnknownResourceIdException, ActiveTransactionAborted,
            ResourceOperationException, InterruptedException {
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        } else if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        } else if (!resources.containsKey(rid)) {
            throw new UnknownResourceIdException(rid);
        }

        // Check for interrupted exception
        long myThreadId = Thread.currentThread().getId();
        long differentId = myThreadId + 1; // TODO Ugly
        if (!resourceLockedBy.containsKey(rid) || resourceLockedBy.getOrDefault(rid, differentId) != myThreadId) {
            if (!lockResource(rid)) {
                waitForResource(rid);
            }
        }

        // I have the resource here, already locked
        operation.execute(resources.get(rid)); // Can throw exception, that's OK

        SuccessfulOperation op = new SuccessfulOperation(rid, operation);
        transactionOperations.get().get(this).addFirst(op);

    }

    private synchronized void cleanup() {
        long myThreadId = Thread.currentThread().getId();
        for (Map.Entry<ResourceId, Long> entry : resourceLockedBy.entrySet()) {
            if (entry.getValue() == myThreadId) {
                resourceLockedBy.remove(entry.getKey());
            }
        }
        notifyAll(); // TODO Good???
        isAborted.remove(myThreadId);
        transactionOperations.get().remove(this);
    }

    @Override
    public void commitCurrentTransaction() throws NoActiveTransactionException, ActiveTransactionAborted {
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        } else if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        }
        cleanup();

    }

    @Override
    public void rollbackCurrentTransaction() {
        if (!isTransactionActive()) {
            return;
        }
        Deque<SuccessfulOperation> toReverse = transactionOperations.get().get(this);

        while (!toReverse.isEmpty()) {
            SuccessfulOperation op = toReverse.pollFirst();
            ResourceOperation operationToReverse = op.getOperation();
            ResourceId rid = op.getResourceId();
            operationToReverse.undo(resources.get(rid));
        }
        cleanup();
    }

    @Override
    public boolean isTransactionActive() {
        return transactionOperations.get().containsKey(this);
    }

    @Override
    public boolean isTransactionAborted() {
        return isTransactionActive() && isAborted.get(Thread.currentThread().getId());
    }
}
