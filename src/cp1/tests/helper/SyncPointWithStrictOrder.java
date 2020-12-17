package cp1.tests.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class SyncPointWithStrictOrder {

    private List<Semaphore> objects;
    private CountDownLatch latch;

    public SyncPointWithStrictOrder(int count) {
        objects = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            objects.set(i, new Semaphore(0));
        latch = new CountDownLatch(count);
    }

    public void arrive(int which, ArriveCb r) throws Exception {
        latch.countDown();
        latch.wait();
        r.run();

        if (which > 0) {
            synchronized (objects.get(which - 1)) {
                objects.get(which - 1).acquire();
            }
            objects.get(which).release();
        }
    }

    public interface ArriveCb {
        void run() throws Exception;
    }

}