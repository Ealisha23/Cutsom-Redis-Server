package Components.Repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * The in-memory keyspace. It stores plain values and knows nothing about the wire
 * protocol. Expiry is lazy: an expired key is dropped the next time it is looked at.
 * The clock is injectable so tests do not need to sleep.
 */
@Component
public class Store {

    private final ConcurrentHashMap<String, Value> map = new ConcurrentHashMap<>();
    private final LongSupplier clock;

    @Autowired
    public Store() {
        this(System::currentTimeMillis);
    }

    public Store(LongSupplier clock) {
        this.clock = clock;
    }

    public void set(String key, String val) {
        set(key, val, -1);
    }

    /** @param ttlMillis time to live in milliseconds, or a negative number for no expiry */
    public void set(String key, String val, long ttlMillis) {
        long expiresAt = ttlMillis >= 0 ? clock.getAsLong() + ttlMillis : Value.NO_EXPIRY;
        map.put(key, new Value(val, expiresAt));
    }

    /** Returns the value, or null if the key does not exist or has expired. */
    public String get(String key) {
        Value value = getValue(key);
        return value == null ? null : value.val();
    }

    public Value getValue(String key) {
        Value value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value.isExpired(clock.getAsLong())) {
            map.remove(key, value); // only removes if nobody replaced it meanwhile
            return null;
        }
        return value;
    }

    /**
     * Atomically increments the integer stored at key (missing keys start at 0) and keeps
     * the key's expiry.
     *
     * @throws NumberFormatException if the value is not a 64-bit integer or would overflow
     */
    public long incr(String key) {
        long[] result = new long[1];
        map.compute(key, (k, old) -> {
            if (old != null && old.isExpired(clock.getAsLong())) {
                old = null;
            }
            long current = old == null ? 0 : Long.parseLong(old.val());
            if (current == Long.MAX_VALUE) {
                throw new NumberFormatException("increment would overflow");
            }
            result[0] = current + 1;
            return new Value(Long.toString(result[0]), old == null ? Value.NO_EXPIRY : old.expiresAt());
        });
        return result[0];
    }

    /** @return 1 if a live key was removed, otherwise 0 */
    public int delete(String key) {
        Value removed = map.remove(key);
        return removed != null && !removed.isExpired(clock.getAsLong()) ? 1 : 0;
    }

    /** Snapshot of the keys that have not expired. */
    public Set<String> getKeys() {
        long now = clock.getAsLong();
        Set<String> keys = new HashSet<>();
        map.forEach((k, v) -> {
            if (!v.isExpired(now)) {
                keys.add(k);
            }
        });
        return keys;
    }

    public void clear() {
        map.clear();
    }
}
