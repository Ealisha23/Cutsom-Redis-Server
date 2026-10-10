package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Repository.Store;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Starts a real master and a real replica on local ports and talks to them over TCP. */
class ReplicationIntegrationTest {

    /** Minimal RESP client for the tests. */
    private static class TestClient implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;

        TestClient(int port) throws IOException {
            socket = new Socket("localhost", port);
            socket.setSoTimeout(5000); // fail instead of hanging
            in = new BufferedInputStream(socket.getInputStream());
        }

        void sendRaw(byte[] bytes) throws IOException {
            socket.getOutputStream().write(bytes);
            socket.getOutputStream().flush();
        }

        void sendCommand(String... args) throws IOException {
            sendRaw(new RespSerializer().encodeCommand(args));
        }

        String call(String... args) throws IOException {
            sendCommand(args);
            return readReply();
        }

        String readReply() throws IOException {
            int type = in.read();
            if (type == -1) {
                throw new IOException("connection closed");
            }
            String line = readLine();
            switch (type) {
                case '+':
                case '-':
                case ':':
                    return (char) type + line;
                case '$': {
                    int length = Integer.parseInt(line);
                    if (length < 0) {
                        return "(nil)";
                    }
                    byte[] data = in.readNBytes(length);
                    in.readNBytes(2);
                    return new String(data, StandardCharsets.UTF_8);
                }
                case '*': {
                    int count = Integer.parseInt(line);
                    List<String> items = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        items.add(readReply());
                    }
                    return items.toString();
                }
                default:
                    throw new IOException("bad reply type " + (char) type);
            }
        }

        private String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            int b;
            while ((b = in.read()) != '\n') {
                if (b == -1) {
                    throw new IOException("connection closed");
                }
                if (b != '\r') {
                    sb.append((char) b);
                }
            }
            return sb.toString();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private BaseTcpServer master;
    private BaseTcpServer replica;
    private int masterPort;
    private int replicaPort;

    private static RedisConfig configFor(int port, String role) {
        RedisConfig config = new RedisConfig();
        config.setPort(port);
        config.setRole(role);
        return config;
    }

    private static CommandHandler handlerFor(RedisConfig config, ConnectionPool pool, RespSerializer serializer) {
        return new CommandHandler(serializer, new Store(), config, pool);
    }

    @BeforeEach
    void startNodes() throws Exception {
        masterPort = freePort();
        replicaPort = freePort();

        RespSerializer serializer = new RespSerializer();

        RedisConfig masterConfig = configFor(masterPort, RedisConfig.ROLE_MASTER);
        ConnectionPool masterPool = new ConnectionPool();
        master = new MasterTcpServer(serializer, handlerFor(masterConfig, masterPool, serializer), masterConfig, masterPool);

        RedisConfig replicaConfig = configFor(replicaPort, RedisConfig.ROLE_SLAVE);
        replicaConfig.setMasterHost("localhost");
        replicaConfig.setMasterPort(masterPort);
        ConnectionPool replicaPool = new ConnectionPool();
        replica = new SlaveTcpServer(serializer, handlerFor(replicaConfig, replicaPool, serializer), replicaConfig, replicaPool);

        Thread masterThread = new Thread(master::startServer, "test-master");
        masterThread.setDaemon(true);
        masterThread.start();
        waitForPort(masterPort);

        Thread replicaThread = new Thread(replica::startServer, "test-replica");
        replicaThread.setDaemon(true);
        replicaThread.start();
        waitForPort(replicaPort);

        waitUntilReplicaIsConnected();
    }

    @AfterEach
    void stopNodes() {
        if (replica != null) {
            replica.stop();
        }
        if (master != null) {
            master.stop();
        }
    }

    private static void waitForPort(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket("localhost", port)) {
                return;
            } catch (IOException e) {
                Thread.sleep(25);
            }
        }
        throw new AssertionError("server on port " + port + " did not start");
    }

    /** WAIT returns 0 until the replica has finished its handshake with the master. */
    private void waitUntilReplicaIsConnected() throws Exception {
        long deadline = System.currentTimeMillis() + 8000;
        try (TestClient c = new TestClient(masterPort)) {
            while (System.currentTimeMillis() < deadline) {
                if (c.call("WAIT", "1", "100").equals(":1")) {
                    return;
                }
                Thread.sleep(50);
            }
        }
        throw new AssertionError("replica never connected to the master");
    }

    @Test
    void masterAndReplicaReportTheirRoles() throws Exception {
        try (TestClient m = new TestClient(masterPort); TestClient r = new TestClient(replicaPort)) {
            assertTrue(m.call("INFO", "replication").contains("role:master"));
            assertTrue(r.call("INFO", "replication").contains("role:slave"));
        }
    }

    @Test
    void writesOnTheMasterAppearOnTheReplica() throws Exception {
        try (TestClient m = new TestClient(masterPort); TestClient r = new TestClient(replicaPort)) {
            assertEquals("+OK", m.call("SET", "foo", "123"));
            assertEquals("+OK", m.call("SET", "bar", "456", "px", "60000"));
            assertEquals(":1", m.call("INCR", "counter"));
            assertEquals(":2", m.call("INCR", "counter"));
            assertEquals(":3", m.call("INCR", "counter"));
            assertEquals(":1", m.call("WAIT", "1", "2000"));

            assertEquals("123", r.call("GET", "foo"));
            assertEquals("456", r.call("GET", "bar"));
            assertEquals("3", r.call("GET", "counter"));
        }
    }

    @Test
    void deletesAreReplicatedToo() throws Exception {
        try (TestClient m = new TestClient(masterPort); TestClient r = new TestClient(replicaPort)) {
            m.call("SET", "gone", "soon");
            assertEquals(":1", m.call("WAIT", "1", "2000"));
            assertEquals("soon", r.call("GET", "gone"));

            assertEquals(":1", m.call("DEL", "gone"));
            assertEquals(":1", m.call("WAIT", "1", "2000"));
            assertEquals("(nil)", r.call("GET", "gone"));
        }
    }

    @Test
    void theReplicaRefusesWrites() throws Exception {
        try (TestClient r = new TestClient(replicaPort)) {
            assertTrue(r.call("SET", "k", "v").startsWith("-READONLY"));
            assertEquals("(nil)", r.call("GET", "k"));
        }
    }

    @Test
    void transactionsReplicateAfterExec() throws Exception {
        try (TestClient m = new TestClient(masterPort); TestClient r = new TestClient(replicaPort)) {
            assertEquals("+OK", m.call("MULTI"));
            assertEquals("+QUEUED", m.call("SET", "t", "10"));
            assertEquals("+QUEUED", m.call("INCR", "t"));
            assertEquals("[+OK, :11]", m.call("EXEC"));
            assertEquals(":1", m.call("WAIT", "1", "2000"));
            assertEquals("11", r.call("GET", "t"));
        }
    }

    @Test
    void concurrentWritersReachTheReplicaInTheSameOrderTheyWereApplied() throws Exception {
        List<Thread> writers = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            Thread writer = new Thread(() -> {
                try (TestClient m = new TestClient(masterPort)) {
                    for (int i = 0; i < 100; i++) {
                        m.call("INCR", "hits");
                        m.call("SET", "last", "x");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            writers.add(writer);
            writer.start();
        }
        for (Thread w : writers) {
            w.join();
        }

        try (TestClient m = new TestClient(masterPort); TestClient r = new TestClient(replicaPort)) {
            assertEquals(":1", m.call("WAIT", "1", "5000"));
            assertEquals("400", m.call("GET", "hits"));
            assertEquals("400", r.call("GET", "hits"));
        }
    }

    @Test
    void waitWorksRepeatedlyAndWithNoNewWrites() throws Exception {
        try (TestClient m = new TestClient(masterPort)) {
            m.call("SET", "a", "1");
            assertEquals(":1", m.call("WAIT", "1", "2000"));
            assertEquals(":1", m.call("WAIT", "1", "2000")); // nothing new: answers straight away
            m.call("SET", "a", "2");
            assertEquals(":1", m.call("WAIT", "1", "2000"));
        }
    }

    @Test
    void manyClientsAtOnceAreAllServed() throws Exception {
        // more simultaneous, blocked connections than a small thread pool could handle
        List<TestClient> clients = new ArrayList<>();
        try {
            for (int i = 0; i < 64; i++) {
                clients.add(new TestClient(masterPort));
            }
            for (TestClient c : clients) {
                assertEquals("+PONG", c.call("PING"));
            }
        } finally {
            for (TestClient c : clients) {
                c.close();
            }
        }
    }

    @Test
    void aMalformedRequestGetsAnErrorAndDoesNotKillTheServer() throws Exception {
        try (TestClient bad = new TestClient(masterPort)) {
            bad.sendRaw("*1\r\n$abc\r\n".getBytes(StandardCharsets.UTF_8));
            assertTrue(bad.readReply().startsWith("-ERR Protocol error"));
        }
        try (TestClient good = new TestClient(masterPort)) {
            assertEquals("+PONG", good.call("PING"));
        }
    }

    @Test
    void aClientThatDisconnectsDoesNotBreakOthers() throws Exception {
        try (TestClient gone = new TestClient(masterPort)) {
            gone.call("PING");
        }
        Thread.sleep(100);
        try (TestClient other = new TestClient(masterPort)) {
            assertEquals("+PONG", other.call("PING"));
        }
    }

    @Test
    void commandsSplitAcrossPacketsAreStillUnderstood() throws Exception {
        try (TestClient m = new TestClient(masterPort)) {
            byte[] all = new RespSerializer().encodeCommand(new String[] {"SET", "split", "value"});
            for (byte b : all) {
                m.sendRaw(new byte[] {b});
                Thread.sleep(1);
            }
            assertEquals("+OK", m.readReply());
            assertEquals("value", m.call("GET", "split"));
        }
    }

    @Test
    void pipelinedCommandsInOneWriteAreAllAnswered() throws Exception {
        try (TestClient m = new TestClient(masterPort)) {
            RespSerializer s = new RespSerializer();
            String batch = s.respArray(new String[] {"SET", "p", "1"})
                    + s.respArray(new String[] {"INCR", "p"})
                    + s.respArray(new String[] {"GET", "p"});
            m.sendRaw(batch.getBytes(StandardCharsets.UTF_8));
            assertEquals("+OK", m.readReply());
            assertEquals(":2", m.readReply());
            assertEquals("2", m.readReply());
        }
    }
}
