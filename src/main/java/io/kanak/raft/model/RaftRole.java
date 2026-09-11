package io.kanak.raft.model;

/**
 * Raft node lifecycle roles as defined in Diego Ongaro's Raft specification.
 */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
