package Components.Repository;

/** A stored string and the moment (epoch millis) it expires. */
public record Value(String val, long expiresAt) {

    public static final long NO_EXPIRY = Long.MAX_VALUE;

    public boolean isExpired(long nowMillis) {
        return expiresAt != NO_EXPIRY && nowMillis >= expiresAt;
    }
}
