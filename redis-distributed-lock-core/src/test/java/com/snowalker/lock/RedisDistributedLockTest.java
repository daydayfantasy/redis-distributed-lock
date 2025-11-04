import org.junit.Test;

import com.snowalker.lock.RedisDistributedLock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class RedisDistributedLockTest {

    @Test
    public void getInstance_shouldReturnSameInstanceInMultipleThreads() throws InterruptedException {
        int threadCount = 100;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final List<RedisDistributedLock> instances = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        RedisDistributedLock instance = RedisDistributedLock.getInstance();
                        synchronized (instances) {
                            instances.add(instance);
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        latch.await();
        executor.shutdown();

        // Check all instances are the same
        RedisDistributedLock firstInstance = instances.get(0);
        for (RedisDistributedLock instance : instances) {
            assertSame("All instances should be the same", firstInstance, instance);
        }

        System.out.println("Test passed: All " + threadCount + " threads got the same instance.");
    }
}