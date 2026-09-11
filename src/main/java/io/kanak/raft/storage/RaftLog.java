package io.kanak.raft.storage;

import io.kanak.raft.model.Command;
import io.kanak.raft.model.LogEntry;
import io.kanak.raft.model.Snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Thread-safe contiguous Raft log with snapshot compaction support.
 * Indexing is 1-based as specified by Diego Ongaro's Raft thesis.
 */
public class RaftLog {

    private long lastIncludedIndex = 0;
    private long lastIncludedTerm = 0;
    private final List<LogEntry> entries = new ArrayList<>();
    private Snapshot latestSnapshot = null;

    public synchronized long getLastLogIndex() {
        if (entries.isEmpty()) {
            return lastIncludedIndex;
        }
        return entries.get(entries.size() - 1).index();
    }

    public synchronized long getLastLogTerm() {
        if (entries.isEmpty()) {
            return lastIncludedTerm;
        }
        return entries.get(entries.size() - 1).term();
    }

    public synchronized long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public synchronized long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public synchronized long getTerm(long index) {
        if (index <= 0) {
            return 0;
        }
        if (index == lastIncludedIndex) {
            return lastIncludedTerm;
        }
        if (index < lastIncludedIndex) {
            return lastIncludedTerm; // Part of compacted history
        }
        int physicalIndex = toPhysicalIndex(index);
        if (physicalIndex < 0 || physicalIndex >= entries.size()) {
            return 0;
        }
        return entries.get(physicalIndex).term();
    }

    public synchronized LogEntry getEntry(long index) {
        if (index <= lastIncludedIndex || index > getLastLogIndex()) {
            return null;
        }
        int physical = toPhysicalIndex(index);
        if (physical < 0 || physical >= entries.size()) {
            return null;
        }
        return entries.get(physical);
    }

    public synchronized long append(long term, Command command) {
        long newIndex = getLastLogIndex() + 1;
        LogEntry entry = new LogEntry(newIndex, term, command);
        entries.add(entry);
        return newIndex;
    }

    public synchronized void appendEntries(List<LogEntry> newEntries) {
        for (LogEntry entry : newEntries) {
            long idx = entry.index();
            if (idx <= lastIncludedIndex) {
                // Entry already compacted into snapshot
                continue;
            }
            if (idx <= getLastLogIndex()) {
                long existingTerm = getTerm(idx);
                if (existingTerm != entry.term()) {
                    // Conflict detected: truncate from idx
                    truncateFrom(idx);
                    entries.add(entry);
                }
                // If existing entry has identical term, do nothing (idempotent duplicate)
            } else {
                entries.add(entry);
            }
        }
    }

    public synchronized void truncateFrom(long fromIndex) {
        if (fromIndex <= lastIncludedIndex) {
            return; // Cannot truncate into compacted history
        }
        int physical = toPhysicalIndex(fromIndex);
        if (physical >= 0 && physical < entries.size()) {
            while (entries.size() > physical) {
                entries.remove(entries.size() - 1);
            }
        }
    }

    public synchronized List<LogEntry> getEntriesFrom(long fromIndex, int maxCount) {
        if (fromIndex <= lastIncludedIndex || fromIndex > getLastLogIndex()) {
            return Collections.emptyList();
        }
        int physical = toPhysicalIndex(fromIndex);
        if (physical < 0 || physical >= entries.size()) {
            return Collections.emptyList();
        }
        int count = Math.min(maxCount, entries.size() - physical);
        return new ArrayList<>(entries.subList(physical, physical + count));
    }

    public synchronized Snapshot takeSnapshot(long snapshotIndex, byte[] stateData) {
        if (snapshotIndex <= lastIncludedIndex || snapshotIndex > getLastLogIndex()) {
            return latestSnapshot;
        }
        long snapshotTerm = getTerm(snapshotIndex);
        int discardCount = toPhysicalIndex(snapshotIndex) + 1;
        if (discardCount > 0 && discardCount <= entries.size()) {
            entries.subList(0, discardCount).clear();
        } else {
            entries.clear();
        }

        lastIncludedIndex = snapshotIndex;
        lastIncludedTerm = snapshotTerm;
        latestSnapshot = new Snapshot(lastIncludedIndex, lastIncludedTerm, stateData);
        return latestSnapshot;
    }

    public synchronized void installSnapshot(Snapshot snapshot) {
        if (snapshot.lastIncludedIndex() <= lastIncludedIndex) {
            return; // Stale snapshot
        }

        if (snapshot.lastIncludedIndex() >= getLastLogIndex()) {
            entries.clear();
        } else {
            int retainStart = toPhysicalIndex(snapshot.lastIncludedIndex()) + 1;
            if (retainStart > 0 && retainStart < entries.size()) {
                entries.subList(0, retainStart).clear();
            } else {
                entries.clear();
            }
        }

        lastIncludedIndex = snapshot.lastIncludedIndex();
        lastIncludedTerm = snapshot.lastIncludedTerm();
        latestSnapshot = snapshot;
    }

    public synchronized Snapshot getLatestSnapshot() {
        return latestSnapshot;
    }

    public synchronized int size() {
        return entries.size();
    }

    private int toPhysicalIndex(long logicalIndex) {
        return (int) (logicalIndex - lastIncludedIndex - 1);
    }
}
