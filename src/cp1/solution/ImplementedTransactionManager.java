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

public class ImplementedTransactionManager implements TransactionManager {
    private static class SuccessfulOperation {
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
            } else if (other.startTime < startTime) { // Other younger than this
                return -1;
            } else {
                if (other.threadId > threadId) { // Same start time, we compare threadIds
                    return 1;
                } else {
                    return -1;
                }
            }

        }
    }

    private LocalTimeProvider timeProvider;
    private Map<ResourceId, Resource> resources;
    private Map<ResourceId, Long> resourceLockedBy; // Thread with id = value() is in control of Resource with resourceId = key()
    private Map<Long, ResourceId> waitsForResource; // Thread with id = key() waits for access to Resource with resourceId = value()
    private Map<Long, TransactionStartTime> startTime; // TransactionStartTime object associated with Thread with id = value()
    private Map<Long, Boolean> isAborted; // True if Thread with if = key() is aborted, false otherwise

    // Map local for each thread, keeps track of successful operations in TransactionManager = key()
    private static ThreadLocal<Map<ImplementedTransactionManager, Deque<SuccessfulOperation>>> transactionOperations = ThreadLocal.withInitial(HashMap::new);

    public ImplementedTransactionManager(Collection<Resource> resources, LocalTimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        this.resources = new ConcurrentHashMap<>();
        for (Resource resource : resources) {
            this.resources.put(resource.getId(), resource);
        }

        this.resourceLockedBy = new ConcurrentHashMap<>();
        this.waitsForResource = new ConcurrentHashMap<>();
        this.startTime = new ConcurrentHashMap<>();
        this.isAborted = new ConcurrentHashMap<>();
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
    private synchronized boolean lockResource(ResourceId rid) {
        if (!resourceLockedBy.containsKey(rid)) {
            resourceLockedBy.put(rid, Thread.currentThread().getId());
            waitsForResource.remove(Thread.currentThread().getId());
            return true;
        } else {
            return false;
        }
    }

    /**
     * Finds a cycle in a directed graph of waiting threads. Each thread can wait only
     * for one resource, so each node has at most one edge going from it. There is only one
     * path to check starting from the current thread.
     *
     * @return Collection of ThreadIds that are part of the cycle, empty if there
     * is no cycle.
     */
    private Collection<Long> findCycle() {
        List<Long> cycle = new ArrayList<>();
        boolean endOfPath = false;
        long start = Thread.currentThread().getId();
        cycle.add(start);
        ResourceId waitingFor = waitsForResource.get(start);
        long next = resourceLockedBy.get(waitingFor);

        // Aborted threads don't wait for resources, they can't create cycles
        while (!isAborted.get(next) && !endOfPath) {
            cycle.add(next);
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

        // Thread can't wait for itself, cycle must be of length > 1, we check equality of first and last thread
        if (cycle.size() != 1 && cycle.get(0).equals(cycle.get(cycle.size() - 1))) {
            return cycle;
        } else { // No cycle found
            return new ArrayDeque<>();
        }
    }

    /**
     * Aborts the youngest thread in a cycle.
     *
     * @param cycle Collection of ThreadIds that are part of the cycle, empty if there
     * is no cycle.
     */
    private void abortYoungest(Collection<Long> cycle) {
        if (cycle.size() != 0) {
            List<TransactionStartTime> candidates = new ArrayList<>();
            for (long threadId : cycle) {
                candidates.add(startTime.get(threadId));
            }
            Collections.sort(candidates); // Sorting by age and threadId
            long toAbort = candidates.get(0).getThreadId(); // Youngest is first

            isAborted.put(toAbort, true);
            for (Thread t : Thread.getAllStackTraces().keySet()) { // We find the thread to interrupt
                if (t.getId() == toAbort) {
                    t.interrupt();
                    break;
                }
            }

        }

    }
    /**
     * Tries to acquire a permission to use given Resource. If the resource is controlled
     * by other transaction, we wait for it to be free. If we add a new edge to the graph of
     * waiting, we need to check for cycle and potentially abort a transaction.
     *
     * @param rid Id of the Resource we want to acquire.
     */
    private synchronized void waitForResource(ResourceId rid) throws InterruptedException, ActiveTransactionAborted {
        long myThreadId = Thread.currentThread().getId();

        while (!lockResource(rid)) {
            if (!waitsForResource.containsKey(myThreadId)) {
                waitsForResource.put(myThreadId, rid);
                Collection<Long> cycle = findCycle();
                abortYoungest(cycle); // Does nothing if there is no cycle
                if (isTransactionAborted()) {
                    throw new ActiveTransactionAborted();
                } else if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
            }
            try {
                wait();
            } catch (InterruptedException interrupted) {
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

        long myThreadId = Thread.currentThread().getId();
        long differentId = myThreadId + 1; // Always different from myThreadId

        // We don't enter if we have previously locked the resource
        if (resourceLockedBy.getOrDefault(rid, differentId) != myThreadId) {
            if (!lockResource(rid)) {
                waitForResource(rid);
            }
        }

        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        operation.execute(resources.get(rid)); // Can throw ResourceOperationException, below code won't be executed

        SuccessfulOperation op = new SuccessfulOperation(rid, operation);
        transactionOperations.get().get(this).addFirst(op);
    }


    /**
     * Cleans up after a transaction is ended. Removes information that is no longer necessary.
     */
    private synchronized void cleanup() {
        long myThreadId = Thread.currentThread().getId();
        for (Map.Entry<ResourceId, Long> entry : resourceLockedBy.entrySet()) {
            if (entry.getValue() == myThreadId) {
                resourceLockedBy.remove(entry.getKey()); // Unlocks resources that were in control of this thread
            }
        }
        waitsForResource.remove(myThreadId);
        isAborted.remove(myThreadId);
        transactionOperations.get().remove(this);
        startTime.remove(myThreadId);
        notifyAll(); // Wakes up other threads, they can lock the released resources
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

        while (!toReverse.isEmpty()) { // We reverse every successful operation that we did
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
