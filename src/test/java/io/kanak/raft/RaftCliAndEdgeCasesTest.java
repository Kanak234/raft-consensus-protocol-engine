package io.kanak.raft;

import io.kanak.raft.cli.RaftCli;
import io.kanak.raft.model.Command;
import io.kanak.raft.model.LogEntry;
import io.kanak.raft.model.RaftRole;
import io.kanak.raft.model.Snapshot;
import io.kanak.raft.rpc.AppendEntriesArgs;
import io.kanak.raft.rpc.InstallSnapshotArgs;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class RaftCliAndEdgeCasesTest {

    @Test
    void testCommandModel() {
        Command putCmd = Command.put("foo", "bar");
        assertEquals("PUT", putCmd.action());
        assertEquals("foo", putCmd.key());
        assertEquals("bar", putCmd.value());

        Command getCmd = Command.get("foo");
        assertEquals("GET", getCmd.action());
        assertEquals("foo", getCmd.key());
        assertNull(getCmd.value());

        Command delCmd = Command.delete("foo");
        assertEquals("DELETE", delCmd.action());
        assertEquals("foo", delCmd.key());

        assertThrows(NullPointerException.class, () -> new Command(null, "k", "v", "c", 0));
        assertThrows(NullPointerException.class, () -> new Command("PUT", null, "v", "c", 0));
    }

    @Test
    void testSnapshotModel() {
        byte[] payload = new byte[]{1, 2, 3, 4};
        Snapshot snapshot = new Snapshot(5, 2, payload);
        assertEquals(5, snapshot.lastIncludedIndex());
        assertEquals(2, snapshot.lastIncludedTerm());
        assertArrayEquals(payload, snapshot.data());

        Snapshot snapshot2 = new Snapshot(5, 2, new byte[]{1, 2, 3, 4});
        assertEquals(snapshot, snapshot2);
        assertEquals(snapshot.hashCode(), snapshot2.hashCode());

        assertThrows(IllegalArgumentException.class, () -> new Snapshot(-1, 0, payload));
        assertThrows(IllegalArgumentException.class, () -> new Snapshot(0, -1, payload));
        assertThrows(NullPointerException.class, () -> new Snapshot(0, 0, null));
    }

    @Test
    void testRpcArgs() {
        AppendEntriesArgs appendArgs = new AppendEntriesArgs(
                1, "node1", 0, 0, Collections.emptyList(), 0
        );
        assertEquals(1, appendArgs.term());
        assertEquals("node1", appendArgs.leaderId());
        assertEquals(0, appendArgs.entries().size());

        byte[] dummyData = new byte[]{1, 2, 3};
        InstallSnapshotArgs snapshotArgs = new InstallSnapshotArgs(
                2, "node1", 10, 2, dummyData
        );
        assertEquals(2, snapshotArgs.term());
        assertEquals("node1", snapshotArgs.leaderId());
        assertEquals(10, snapshotArgs.lastIncludedIndex());
        assertEquals(2, snapshotArgs.lastIncludedTerm());
        assertArrayEquals(dummyData, snapshotArgs.data());
    }

    @Test
    void testRaftCliHelpAndBench() {
        assertDoesNotThrow(() -> RaftCli.main(new String[]{"--help"}));
        assertDoesNotThrow(() -> RaftCli.runBenchmark(5));
    }

    @Test
    void testRaftCliInteractiveCommands() {
        String input = "status\nput k1 v1\nget k1\ntick 10\nheal\nexit\n";
        System.setIn(new ByteArrayInputStream(input.getBytes()));
        try {
            assertDoesNotThrow(() -> RaftCli.main(new String[]{}));
        } finally {
            System.setIn(System.in);
        }
    }
}
