package io.kanak.raft.statemachine;

import io.kanak.raft.model.Command;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Thread-safe ordered in-memory Key-Value State Machine.
 */
public class KeyValueStateMachine implements StateMachine {

    private final ConcurrentSkipListMap<String, String> store = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<String, Long> lastAppliedClientSeq = new ConcurrentHashMap<>();

    @Override
    public synchronized String apply(Command command) {
        if (command == null) return "ERR_NULL_COMMAND";

        // Idempotency check for client requests
        if (command.clientId() != null && command.sequenceNumber() > 0) {
            Long lastSeq = lastAppliedClientSeq.get(command.clientId());
            if (lastSeq != null && lastSeq >= command.sequenceNumber()) {
                // Duplicate command; return cached read or OK
                return store.getOrDefault(command.key(), "OK");
            }
            lastAppliedClientSeq.put(command.clientId(), command.sequenceNumber());
        }

        return switch (command.action().toUpperCase()) {
            case "PUT" -> {
                store.put(command.key(), command.value() != null ? command.value() : "");
                yield "OK";
            }
            case "GET" -> store.get(command.key());
            case "DELETE" -> {
                String removed = store.remove(command.key());
                yield removed != null ? "OK" : "NOT_FOUND";
            }
            default -> "ERR_UNKNOWN_ACTION";
        };
    }

    public synchronized String get(String key) {
        return store.get(key);
    }

    public synchronized Map<String, String> getAll() {
        return Collections.unmodifiableMap(new ConcurrentSkipListMap<>(store));
    }

    @Override
    public synchronized byte[] takeSnapshot() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(new ConcurrentSkipListMap<>(store));
            oos.writeObject(new ConcurrentHashMap<>(lastAppliedClientSeq));
            oos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize state machine snapshot", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void restoreSnapshot(byte[] snapshotData) {
        if (snapshotData == null || snapshotData.length == 0) {
            store.clear();
            lastAppliedClientSeq.clear();
            return;
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(snapshotData);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            var restoredStore = (ConcurrentSkipListMap<String, String>) ois.readObject();
            var restoredSeq = (ConcurrentHashMap<String, Long>) ois.readObject();

            store.clear();
            store.putAll(restoredStore);
            lastAppliedClientSeq.clear();
            lastAppliedClientSeq.putAll(restoredSeq);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize state machine snapshot", e);
        }
    }
}
