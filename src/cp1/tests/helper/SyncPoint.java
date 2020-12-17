package cp1.tests.helper;

import java.util.concurrent.CountDownLatch;

public class SyncPoint {

    CountDownLatch latch;

    public SyncPoint(int i) {
        this.latch = new CountDownLatch(i);
    }

    public void arrived() throws InterruptedException {
        this.latch.countDown();
        this.latch.await();
    }

}
