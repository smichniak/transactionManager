package cp1.tests.helper;

import cp1.base.Resource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StrictResourceControl extends BaseResourceControl {

    private final Map<Resource, Thread> resources = new HashMap<>();
    private final ThreadLocal<Set<Resource>> threadResources = new ThreadLocal<>();

    synchronized void onResourceAcquired(Resource resource) {
        if (resources.get(resource) == Thread.currentThread())
            return;
        if (resources.containsKey(resource))
            throw new IllegalStateException("Thread got access to resources before the other thread fully released them. If you think your implementation is correct, this might be caused by releasing resources too early during cancellation, see https://moodle.mimuw.edu.pl/mod/forum/discuss.php?d=3922. If you are making a custom test, ensure that you use the wrapper function provided by Helper, or disable strict resource control for your test.");
        resources.put(resource, Thread.currentThread());
        if (threadResources.get() == null)
            threadResources.set(new HashSet<>());
        threadResources.get().add(resource);
    }

    synchronized void onResourcesReleased() {
        Set<Resource> s = threadResources.get();
        if (s != null) {
            for (Resource r : s) {
                resources.remove(r);
            }
        }
        threadResources.remove();
    }

}
