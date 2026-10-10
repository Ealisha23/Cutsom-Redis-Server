package Components.Repository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class StoreTest {
    private AtomicLong now;
    private Store store;

    @BeforeEach
    void setUp() {
        now = new AtomicLong(1_000_000);
        store = new Store(now::get); // a controllable clock: no Thread.sleep needed
    }

    @Test
    void setAndGet() {
        store.set("testKey", "testValue");
        assertEquals("testValue", store.get("testKey"));
    }

    @Test
    void missingKeyIsNull() {
        assertNull(store.get("nope"));
    }

    @Test
    void overwritingAKeyReplacesTheValue() {
        store.set("k", "one");
        store.set("k", "two");
        assertEquals("two", store.get("k"));
    }

    @Test
    void keyExpiresExactlyAtItsDeadline() {
        store.set("k", "v", 100);
        now.addAndGet(99);
        assertEquals("v", store.get("k"));
        now.addAndGet(1);
        assertNull(store.get("k"));
    }

    @Test
    void settingAgainResetsTheExpiry() {
        store.set("k", "old", 100);
        now.addAndGet(60);
        store.set("k", "new", 500);
        now.addAndGet(100); // the first deadline has passed, the second has not
        assertEquals("new", store.get("k"));
    }

    @Test
    void settingWithoutExpiryClearsAnOldExpiry() {
        store.set("k", "old", 100);
        store.set("k", "new");
        now.addAndGet(10_000);
        assertEquals("new", store.get("k"));
    }

    @Test
    void incrStartsAtOneAndCounts() {
        assertEquals(1, store.incr("n"));
        assertEquals(2, store.incr("n"));
        assertEquals("2", store.get("n"));
    }

    @Test
    void incrKeepsTheExpiry() {
        store.set("n", "5", 100);
        assertEquals(6, store.incr("n"));
        now.addAndGet(100);
        assertNull(store.get("n"));
    }

    @Test
    void incrOnAnExpiredKeyStartsOver() {
        store.set("n", "5", 100);
        now.addAndGet(100);
        assertEquals(1, store.incr("n"));
    }

    @Test
    void incrRejectsNonIntegersAndLeavesTheValueAlone() {
        store.set("s", "abc");
        assertThrows(NumberFormatException.class, () -> store.incr("s"));
        assertEquals("abc", store.get("s"));
    }

    @Test
    void incrRejectsOverflow() {
        store.set("big", String.valueOf(Long.MAX_VALUE));
        assertThrows(NumberFormatException.class, () -> store.incr("big"));
    }

    @Test
    void deleteReportsWhetherAKeyWasRemoved() {
        store.set("k", "v");
        assertEquals(1, store.delete("k"));
        assertEquals(0, store.delete("k"));
        assertNull(store.get("k"));
    }

    @Test
    void deletingAnExpiredKeyCountsAsNothingRemoved() {
        store.set("k", "v", 10);
        now.addAndGet(10);
        assertEquals(0, store.delete("k"));
    }

    @Test
    void getKeysSkipsExpiredKeys() {
        store.set("a", "1");
        store.set("b", "2", 10);
        now.addAndGet(10);
        assertEquals(1, store.getKeys().size());
        assertTrue(store.getKeys().contains("a"));
    }

    private static void runInParallel(int threads, Runnable work) throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                work.run();
            });
            workers.add(t);
            t.start();
        }
        start.countDown(); // release everyone at once
        for (Thread t : workers) {
            t.join();
        }
    }

    @Test
    void concurrentSetsOfDifferentKeysAreAllKept() throws InterruptedException {
        AtomicLong threadNumber = new AtomicLong();
        runInParallel(10, () -> {
            long id = threadNumber.getAndIncrement();
            for (int j = 0; j < 100; j++) {
                store.set("key" + j + "," + id, "ooga booga");
            }
        });
        assertEquals(1000, store.getKeys().size());
    }

    @Test
    void concurrentIncrementsNeverLoseAnUpdate() throws InterruptedException {
        runInParallel(8, () -> {
            for (int j = 0; j < 500; j++) {
                store.incr("counter");
            }
        });
        assertEquals("4000", store.get("counter"));
    }
}
