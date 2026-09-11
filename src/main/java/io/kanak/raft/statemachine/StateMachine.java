package io.kanak.raft.statemachine;

import io.kanak.raft.model.Command;

/**
 * Replicated state machine interface driven by committed Raft log entries.
 */
public interface StateMachine {

    /**
     * Applies a committed command to the state machine.
     *
     * @param command the committed command
     * @return the execution result
     */
    String apply(Command command);

    /**
     * Returns the current state serialized as a byte array for snapshotting.
     */
    byte[] takeSnapshot();

    /**
     * Restores the state machine from a snapshot.
     */
    void restoreSnapshot(byte[] snapshotData);
}
