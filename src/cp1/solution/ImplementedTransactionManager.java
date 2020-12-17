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
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    private Map<ResourceId, Boolean> isResourceLocked;
    private LocalTimeProvider timeProvider;
    private Map<ResourceId, Resource> resources;
    private Map<Long, Boolean> aborted; // ThreadId as key
    private Semaphore resourceLockMutex = new Semaphore(1);

    private static ThreadLocal<Map<ImplementedTransactionManager, Set<ResourceId>>> transactions = ThreadLocal.withInitial(HashMap::new);
    private static ThreadLocal<Map<ImplementedTransactionManager, Deque<SuccessfulOperation>>> transactionOperations = ThreadLocal.withInitial(HashMap::new);

    /**
     * Checks if a resource with a given Id is locked and tries
     * to lock it, if it isn't.
     *
     * @param rid Id of the resource we want to lock.
     * @return True if we locked the resource, false if we didn't.
     */
    private boolean lockResource(ResourceId rid) throws InterruptedException {
        resourceLockMutex.acquire();
        if (!isResourceLocked.get(rid)) {
            isResourceLocked.put(rid, true);
            resourceLockMutex.release();
            return true;
        } else {
            resourceLockMutex.release();
            return false;
        }

    }

    public ImplementedTransactionManager(Collection<Resource> resources, LocalTimeProvider timeProvider) {
        this.isResourceLocked = new ConcurrentHashMap<>();
        this.resources = new ConcurrentHashMap<>(); // Concurrent or not?
        for (Resource resource : resources) {
            this.isResourceLocked.put(resource.getId(), false);
            this.resources.put(resource.getId(), resource); // TODO nullptr exception
        }

        this.timeProvider = timeProvider;
        this.aborted = new ConcurrentHashMap<>();
    }

    @Override
    public void startTransaction() throws AnotherTransactionActiveException {
        Map<ImplementedTransactionManager, Set<ResourceId>> startedTransactions = transactions.get();
        if (startedTransactions.containsKey(this)) {
            throw new AnotherTransactionActiveException();
        }
        startedTransactions.put(this, new HashSet<>());
        transactionOperations.get().put(this, new ArrayDeque<>());
        aborted.put(Thread.currentThread().getId(), false);
    }

    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation) throws
            NoActiveTransactionException, UnknownResourceIdException, ActiveTransactionAborted,
            ResourceOperationException, InterruptedException {
        if (!isTransactionActive()) {
            throw new NoActiveTransactionException();
        } else if (isTransactionAborted()) {
            throw new ActiveTransactionAborted();
        } else if (!isResourceLocked.containsKey(rid)) {
            throw new UnknownResourceIdException(rid);
        }
        Set<ResourceId> myResources = transactions.get().get(this);

        // Check for interrupted exception
        if (!lockResource(rid)) {
            // Waiting for
        }

        // I have the resource here, already locked
        transactions.get().get(this).add(rid);
        try {
            operation.execute(resources.get(rid));

            SuccessfulOperation op = new SuccessfulOperation(rid, operation);
            transactionOperations.get().get(this).addFirst(op);

        } catch (ResourceOperationException e) {

        }


    }

    private void cleanup() {
        Set<ResourceId> myResources = transactions.get().get(this);
        for (ResourceId rid : myResources) {
            isResourceLocked.put(rid, false);
        }
        aborted.remove(Thread.currentThread().getId());
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
        return transactions.get().containsKey(this);
    }

    @Override
    public boolean isTransactionAborted() {
        return isTransactionActive() && aborted.get(Thread.currentThread().getId());
    }
}
