package io.kanak.raft.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Encapsulates a state machine command submitted by a client.
 */
public record Command(
        String action,
        String key,
        String value,
        String clientId,
        long sequenceNumber
) implements Serializable {

    public Command {
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(key, "key must not be null");
    }

    public static Command put(String key, String value) {
        return new Command("PUT", key, value, "anonymous", 0);
    }

    public static Command get(String key) {
        return new Command("GET", key, null, "anonymous", 0);
    }

    public static Command delete(String key) {
        return new Command("DELETE", key, null, "anonymous", 0);
    }
}
