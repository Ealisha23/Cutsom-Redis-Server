package Components.Service;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Repository.Store;
import Components.Server.RedisConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CommandHandlerTest {
    private RespSerializer serializer;
    private Store store;
    private RedisConfig config;
    private ConnectionPool pool;
    private CommandHandler handler;
    private Client client;
    private int nextId;

    @BeforeEach
    void setUp() {
        serializer = new RespSerializer();
        store = new Store();
        config = new RedisConfig();
        pool = new ConnectionPool();
        handler = new CommandHandler(serializer, store, config, pool);
        client = newClient(null);
    }

    /** A client whose output goes to a buffer instead of a socket. */
    private Client newClient(ByteArrayOutputStream out) {
        return new Client(null, new ByteArrayInputStream(new byte[0]),
                out == null ? new ByteArrayOutputStream() : out, ++nextId);
    }

    private String run(String... command) {
        return runAs(client, command);
    }

    private String runAs(Client who, String... command) {
        ResponseDto reply = handler.execute(command, who);
        return reply == null ? null : reply.response;
    }

    // ------------------------------------------------------------------ basics

    @Test
    void ping() {
        assertEquals("+PONG\r\n", run("PING"));
        assertEquals("$2\r\nhi\r\n", run("PING", "hi"));
    }

    @Test
    void commandNamesAreCaseInsensitive() {
        assertEquals("+PONG\r\n", run("ping"));
        assertEquals("+OK\r\n", run("set", "k", "v"));
        assertEquals("$1\r\nv\r\n", run("gEt", "k"));
    }

    @Test
    void echo() {
        assertEquals("$3\r\nhey\r\n", run("ECHO", "hey"));
        assertEquals("-ERR wrong number of arguments for 'echo' command\r\n", run("ECHO"));
    }

    @Test
    void setAndGet() {
        assertEquals("+OK\r\n", run("SET", "foo", "bar"));
        assertEquals("$3\r\nbar\r\n", run("GET", "foo"));
        assertEquals("$-1\r\n", run("GET", "missing"));
    }

    @Test
    void setWithPxAndExAnyCase() {
        assertEquals("+OK\r\n", run("SET", "a", "1", "px", "100"));
        assertEquals("+OK\r\n", run("SET", "b", "1", "PX", "100"));
        assertEquals("+OK\r\n", run("SET", "c", "1", "ex", "10"));
        assertEquals("$1\r\n1\r\n", run("GET", "a"));
    }

    @Test
    void setRejectsBadOptions() {
        assertEquals("-ERR syntax error\r\n", run("SET", "a", "1", "PX"));
        assertEquals("-ERR syntax error\r\n", run("SET", "a", "1", "BOGUS"));
        assertEquals("-ERR invalid expire time in 'set' command\r\n", run("SET", "a", "1", "PX", "0"));
        assertEquals("-ERR value is not an integer or out of range\r\n", run("SET", "a", "1", "PX", "abc"));
        assertEquals("-ERR wrong number of arguments for 'set' command\r\n", run("SET", "a"));
    }

    @Test
    void valuesWithSpacesAndUnicodeSurvive() {
        run("SET", "k", "hello wor\u00e9ld");
        assertEquals("$13\r\nhello wor\u00e9ld\r\n", run("GET", "k")); // 12 characters, 13 bytes
    }

    @Test
    void incr() {
        assertEquals(":1\r\n", run("INCR", "n"));
        assertEquals(":2\r\n", run("INCR", "n"));
        run("SET", "s", "abc");
        assertEquals("-ERR value is not an integer or out of range\r\n", run("INCR", "s"));
    }

    @Test
    void del() {
        run("SET", "a", "1");
        run("SET", "b", "2");
        assertEquals(":2\r\n", run("DEL", "a", "b", "never-existed"));
        assertEquals(":0\r\n", run("DEL", "a"));
    }

    @Test
    void unknownCommandIsAnError() {
        assertEquals("-ERR unknown command 'FLY'\r\n", run("FLY"));
    }

    @Test
    void infoReplicationOnAMaster() {
        String reply = run("INFO", "replication");
        assertTrue(reply.contains("role:master"));
        assertTrue(reply.contains("master_replid:" + config.getMasterReplId()));
        assertTrue(reply.contains("master_repl_offset:0"));
        assertEquals(40, config.getMasterReplId().length());
    }

    @Test
    void infoReplicationOnAReplica() {
        config.setRole(RedisConfig.ROLE_SLAVE);
        assertTrue(run("INFO", "replication").contains("role:slave"));
    }

    // ------------------------------------------------------------ transactions

    @Test
    void multiQueuesAndExecRunsAtomically() {
        assertEquals("+OK\r\n", run("MULTI"));
        assertEquals("+QUEUED\r\n", run("SET", "k", "1"));
        assertEquals("+QUEUED\r\n", run("INCR", "k"));
        assertEquals("+QUEUED\r\n", run("GET", "k"));
        assertNullKey("k");
        assertEquals("*3\r\n+OK\r\n:2\r\n$1\r\n2\r\n", run("EXEC"));
        assertEquals("$1\r\n2\r\n", run("GET", "k"));
    }

    private void assertNullKey(String key) {
        assertNull(store.get(key));
    }

    @Test
    void execOfAnEmptyTransaction() {
        run("MULTI");
        assertEquals("*0\r\n", run("EXEC"));
    }

    @Test
    void execAndDiscardWithoutMulti() {
        assertEquals("-ERR EXEC without MULTI\r\n", run("EXEC"));
        assertEquals("-ERR DISCARD without MULTI\r\n", run("DISCARD"));
    }

    @Test
    void discardDropsTheQueue() {
        run("MULTI");
        run("SET", "k", "1");
        assertEquals("+OK\r\n", run("DISCARD"));
        assertNull(store.get("k"));
        assertEquals("-ERR EXEC without MULTI\r\n", run("EXEC"));
    }

    @Test
    void multiCannotBeNested() {
        run("MULTI");
        assertEquals("-ERR MULTI calls can not be nested\r\n", run("MULTI"));
    }

    @Test
    void anErrorInsideExecDoesNotStopTheOtherCommands() {
        run("SET", "s", "abc");
        run("MULTI");
        run("INCR", "s");
        run("INCR", "n");
        assertEquals("*2\r\n-ERR value is not an integer or out of range\r\n:1\r\n", run("EXEC"));
    }

    @Test
    void transactionsAreIsolatedPerClient() {
        Client other = newClient(null);
        run("MULTI");
        assertEquals("+PONG\r\n", runAs(other, "PING"));
        assertEquals("+OK\r\n", runAs(other, "SET", "k", "other")); // not queued
        assertEquals("other", store.get("k"));
    }

    // ---------------------------------------------------------------- replicas

    @Test
    void replicaRejectsWritesFromClientsButAppliesWritesFromItsMaster() {
        config.setRole(RedisConfig.ROLE_SLAVE);
        assertEquals("-READONLY You can't write against a read only replica.\r\n", run("SET", "k", "v"));
        assertEquals("-READONLY You can't write against a read only replica.\r\n", run("INCR", "k"));
        assertEquals("-READONLY You can't write against a read only replica.\r\n", run("DEL", "k"));

        Client masterLink = newClient(null);
        masterLink.setMasterLink(true);
        assertEquals("+OK\r\n", runAs(masterLink, "SET", "k", "v"));
        assertEquals("$1\r\nv\r\n", run("GET", "k")); // reads are fine
    }

    @Test
    void replconfHandshakeRepliesOk() {
        assertEquals("+OK\r\n", run("REPLCONF", "listening-port", "6380"));
        assertEquals("+OK\r\n", run("REPLCONF", "capa", "psync2"));
        assertEquals(6380, client.getListeningPort());
        assertEquals("psync2", client.getCapabilities().get(0));
    }

    @Test
    void capaBeforeListeningPortNoLongerCrashes() {
        assertEquals("+OK\r\n", run("REPLCONF", "capa", "psync2"));
    }

    @Test
    void getackAnswersWithTheCurrentOffset() {
        config.setMasterReplOffset(37);
        assertEquals("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$2\r\n37\r\n", run("REPLCONF", "GETACK", "*"));
    }

    @Test
    void psyncSendsFullResyncAndRegistersTheReplica() {
        ByteArrayOutputStream replicaOut = new ByteArrayOutputStream();
        Client replica = newClient(replicaOut);

        assertNull(runAs(replica, "PSYNC", "?", "-1")); // reply already written to the replica
        String written = new String(replicaOut.toByteArray(), StandardCharsets.ISO_8859_1);

        assertTrue(written.startsWith("+FULLRESYNC " + config.getMasterReplId() + " 0\r\n$"));
        assertEquals(1, pool.getSlaveCount());
    }

    @Test
    void writesAreStreamedToReplicasInOrder() {
        ByteArrayOutputStream replicaOut = new ByteArrayOutputStream();
        Client replica = newClient(replicaOut);
        runAs(replica, "PSYNC", "?", "-1");
        replicaOut.reset();

        run("SET", "a", "1");
        run("INCR", "a");
        run("GET", "a");      // reads are not streamed
        run("INCR", "fresh"); // succeeds on a new key, so it is streamed
        run("SET", "b", "x", "px", "100");

        String expected = serializer.respArray(new String[] {"SET", "a", "1"})
                + serializer.respArray(new String[] {"INCR", "a"})
                + serializer.respArray(new String[] {"INCR", "fresh"})
                + serializer.respArray(new String[] {"SET", "b", "x", "px", "100"});
        assertEquals(expected, replicaOut.toString(StandardCharsets.UTF_8));
        assertEquals(expected.getBytes(StandardCharsets.UTF_8).length, pool.propagatedBytes());
        assertTrue(run("INFO", "replication").contains("master_repl_offset:" + pool.propagatedBytes()));
    }

    @Test
    void failedWritesAreNotStreamed() {
        ByteArrayOutputStream replicaOut = new ByteArrayOutputStream();
        runAs(newClient(replicaOut), "PSYNC", "?", "-1");
        replicaOut.reset();

        run("SET", "s", "abc");
        replicaOut.reset();
        run("INCR", "s"); // fails: not an integer
        run("SET", "x"); // fails: wrong arguments
        assertEquals(0, replicaOut.size());
    }

    @Test
    void transactionWritesAreStreamedAfterExec() {
        ByteArrayOutputStream replicaOut = new ByteArrayOutputStream();
        runAs(newClient(replicaOut), "PSYNC", "?", "-1");
        replicaOut.reset();

        run("MULTI");
        run("SET", "k", "1");
        run("INCR", "k");
        assertEquals(0, replicaOut.size()); // nothing leaves before EXEC
        run("EXEC");

        String expected = serializer.respArray(new String[] {"SET", "k", "1"})
                + serializer.respArray(new String[] {"INCR", "k"});
        assertEquals(expected, replicaOut.toString(StandardCharsets.UTF_8));
    }

    // -------------------------------------------------------------------- WAIT

    @Test
    void waitWithoutReplicasReturnsZero() {
        assertEquals(":0\r\n", run("WAIT", "1", "100"));
    }

    @Test
    void waitReturnsImmediatelyWhenNothingWasWritten() {
        runAs(newClient(null), "PSYNC", "?", "-1");
        assertEquals(":1\r\n", run("WAIT", "1", "5000"));
    }

    @Test
    void waitTimesOutWhenTheReplicaNeverAnswers() {
        ByteArrayOutputStream replicaOut = new ByteArrayOutputStream();
        runAs(newClient(replicaOut), "PSYNC", "?", "-1");
        run("SET", "k", "v");

        long start = System.nanoTime();
        assertEquals(":0\r\n", run("WAIT", "1", "150"));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMillis >= 140, "returned after only " + elapsedMillis + " ms");

        // the GETACK request was sent to the replica
        assertTrue(replicaOut.toString(StandardCharsets.UTF_8)
                .endsWith(serializer.respArray(new String[] {"REPLCONF", "GETACK", "*"})));
    }

    @Test
    void waitReturnsAsSoonAsTheReplicaAcknowledges() throws Exception {
        Client replica = newClient(null);
        runAs(replica, "PSYNC", "?", "-1");
        run("SET", "k", "v");
        long offsetBeforeGetack = pool.propagatedBytes();

        AtomicReference<String> waitReply = new AtomicReference<>();
        Thread waiter = new Thread(() -> waitReply.set(run("WAIT", "1", "5000")));
        waiter.start();

        Thread.sleep(100); // let WAIT send its GETACK and start blocking
        assertNull(runAs(replica, "REPLCONF", "ACK", String.valueOf(offsetBeforeGetack)));
        waiter.join(3000);

        assertEquals(":1\r\n", waitReply.get());
    }

    @Test
    void anAckForAnOlderOffsetDoesNotCount() {
        Client replica = newClient(null);
        runAs(replica, "PSYNC", "?", "-1");
        run("SET", "k", "v");

        runAs(replica, "REPLCONF", "ACK", "1"); // far behind
        assertEquals(":0\r\n", run("WAIT", "1", "100"));
    }

    @Test
    void waitCountsOnlyReplicasThatCaughtUp() throws Exception {
        Client fast = newClient(null);
        Client slow = newClient(null);
        runAs(fast, "PSYNC", "?", "-1");
        runAs(slow, "PSYNC", "?", "-1");
        run("SET", "k", "v");
        long target = pool.propagatedBytes();

        AtomicReference<String> waitReply = new AtomicReference<>();
        Thread waiter = new Thread(() -> waitReply.set(run("WAIT", "2", "400")));
        waiter.start();
        Thread.sleep(100);
        runAs(fast, "REPLCONF", "ACK", String.valueOf(target));
        waiter.join(3000);

        assertEquals(":1\r\n", waitReply.get());
    }
}
