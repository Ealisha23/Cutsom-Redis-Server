package Components.Service;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.Store;
import Components.Server.RedisConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executes commands and builds the replies.
 *
 * Concurrency model (like real Redis): commands that touch data run one at a time under
 * {@link #commandLock}. That makes MULTI/EXEC atomic and guarantees that the order in which
 * writes are applied is the order in which they are streamed to replicas. Commands that
 * wait (WAIT) or only record an acknowledgement (REPLCONF ACK) do not hold the lock while
 * they wait.
 */
@Component
public class CommandHandler {
    private static final Logger logger = Logger.getLogger(CommandHandler.class.getName());

    private static final String EMPTY_RDB_BASE64 =
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

    private static final Set<String> WRITE_COMMANDS = Set.of("SET", "DEL", "INCR");

    private final ReentrantLock commandLock = new ReentrantLock();

    private final RespSerializer respSerializer;
    private final Store store;
    private final RedisConfig redisConfig;
    private final ConnectionPool connectionPool;

    @Autowired
    public CommandHandler(RespSerializer respSerializer,
                          Store store,
                          RedisConfig redisConfig,
                          ConnectionPool connectionPool) {
        this.respSerializer = respSerializer;
        this.store = store;
        this.redisConfig = redisConfig;
        this.connectionPool = connectionPool;
    }

    /**
     * Executes one command for a client.
     *
     * @return the reply, or null if nothing should be sent back
     */
    public ResponseDto execute(String[] command, Client client) {
        if (command == null || command.length == 0) {
            return error("ERR empty command");
        }
        String name = command[0].toUpperCase(Locale.ROOT);

        switch (name) {
            case "MULTI":
                return multi(client);
            case "EXEC":
                return exec(client);
            case "DISCARD":
                return discard(client);
            default:
                break;
        }

        if (client.isInTransaction()) {
            client.queueCommand(command);
            return new ResponseDto(respSerializer.serializeSimpleString("QUEUED"));
        }
        return executeNow(name, command, client);
    }

    // ------------------------------------------------------------------ dispatch

    private ResponseDto executeNow(String name, String[] command, Client client) {
        try {
            if (WRITE_COMMANDS.contains(name) && redisConfig.isReplica() && !client.isMasterLink()) {
                return error("READONLY You can't write against a read only replica.");
            }
            return switch (name) {
                case "PING" -> ping(command);
                case "ECHO" -> echo(command);
                case "INFO" -> info(command);
                case "COMMAND" -> new ResponseDto(RespSerializer.EMPTY_ARRAY);
                case "REPLCONF" -> replconf(command, client);
                case "PSYNC" -> psync(command, client);
                case "WAIT" -> waitForReplicas(command);
                case "SET", "GET", "DEL", "INCR" -> runLocked(name, command);
                default -> error("ERR unknown command '" + command[0] + "'");
            };
        } catch (NumberFormatException e) {
            return error("ERR value is not an integer or out of range");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return error("ERR interrupted");
        }
    }

    /** Runs a data command under the command lock and streams successful writes to replicas. */
    private ResponseDto runLocked(String name, String[] command) {
        commandLock.lock();
        try {
            ResponseDto response = dispatchData(name, command);
            if (WRITE_COMMANDS.contains(name) && !response.isError()) {
                connectionPool.propagate(respSerializer.encodeCommand(command));
            }
            return response;
        } finally {
            commandLock.unlock();
        }
    }

    private ResponseDto dispatchData(String name, String[] command) {
        return switch (name) {
            case "SET" -> set(command);
            case "GET" -> get(command);
            case "DEL" -> del(command);
            case "INCR" -> incr(command);
            default -> error("ERR unknown command '" + command[0] + "'");
        };
    }

    // ------------------------------------------------------------ basic commands

    private ResponseDto ping(String[] command) {
        if (command.length > 2) {
            return wrongArguments(command[0]);
        }
        if (command.length == 2) {
            return new ResponseDto(respSerializer.serializeBulkString(command[1]));
        }
        return new ResponseDto(respSerializer.serializeSimpleString("PONG"));
    }

    private ResponseDto echo(String[] command) {
        if (command.length != 2) {
            return wrongArguments(command[0]);
        }
        return new ResponseDto(respSerializer.serializeBulkString(command[1]));
    }

    private ResponseDto set(String[] command) {
        if (command.length < 3) {
            return wrongArguments(command[0]);
        }
        long ttlMillis = -1;
        try {
            for (int i = 3; i < command.length; i++) {
                String option = command[i].toUpperCase(Locale.ROOT);
                if (option.equals("EX") || option.equals("PX")) {
                    if (ttlMillis != -1 || i + 1 >= command.length) {
                        return error("ERR syntax error");
                    }
                    long amount = Long.parseLong(command[++i]);
                    if (amount <= 0) {
                        return error("ERR invalid expire time in 'set' command");
                    }
                    ttlMillis = option.equals("EX") ? Math.multiplyExact(amount, 1000L) : amount;
                } else {
                    return error("ERR syntax error");
                }
            }
        } catch (ArithmeticException e) {
            return error("ERR invalid expire time in 'set' command");
        }
        store.set(command[1], command[2], ttlMillis);
        return new ResponseDto(respSerializer.serializeSimpleString("OK"));
    }

    private ResponseDto get(String[] command) {
        if (command.length != 2) {
            return wrongArguments(command[0]);
        }
        return new ResponseDto(respSerializer.serializeBulkString(store.get(command[1])));
    }

    private ResponseDto del(String[] command) {
        if (command.length < 2) {
            return wrongArguments(command[0]);
        }
        int removed = 0;
        for (int i = 1; i < command.length; i++) {
            removed += store.delete(command[i]);
        }
        return new ResponseDto(respSerializer.respInteger(removed));
    }

    private ResponseDto incr(String[] command) {
        if (command.length != 2) {
            return wrongArguments(command[0]);
        }
        return new ResponseDto(respSerializer.respInteger(store.incr(command[1])));
    }

    public ResponseDto info(String[] command) {
        String section = command.length > 1 ? command[1].toLowerCase(Locale.ROOT) : "replication";
        if (!section.equals("replication") && !section.equals("all") && !section.equals("default")) {
            return new ResponseDto(respSerializer.serializeBulkString(""));
        }
        String body = String.join("\r\n",
                "role:" + redisConfig.getRole(),
                "master_replid:" + redisConfig.getMasterReplId(),
                "master_repl_offset:" + currentReplicationOffset());
        return new ResponseDto(respSerializer.serializeBulkString(body));
    }

    private long currentReplicationOffset() {
        return redisConfig.isReplica() ? redisConfig.getMasterReplOffset() : connectionPool.propagatedBytes();
    }

    // -------------------------------------------------------------- transactions

    private ResponseDto multi(Client client) {
        if (!client.beginTransaction()) {
            return error("ERR MULTI calls can not be nested");
        }
        return new ResponseDto(respSerializer.serializeSimpleString("OK"));
    }

    private ResponseDto discard(Client client) {
        if (!client.isInTransaction()) {
            return error("ERR DISCARD without MULTI");
        }
        client.endTransaction();
        return new ResponseDto(respSerializer.serializeSimpleString("OK"));
    }

    private ResponseDto exec(Client client) {
        if (!client.isInTransaction()) {
            return error("ERR EXEC without MULTI");
        }
        List<String[]> queued = client.endTransaction();
        List<String> replies = new ArrayList<>();

        // Holding the lock for the whole batch makes the transaction atomic: no other
        // client's command can run in between, and replicas get the writes back to back.
        commandLock.lock();
        try {
            for (String[] queuedCommand : queued) {
                String queuedName = queuedCommand[0].toUpperCase(Locale.ROOT);
                ResponseDto reply = executeNow(queuedName, queuedCommand, client);
                replies.add(reply == null || reply.response == null ? RespSerializer.NULL_BULK_STRING : reply.response);
            }
        } finally {
            commandLock.unlock();
        }
        return new ResponseDto(respSerializer.respArray(replies));
    }

    // -------------------------------------------------------------- replication

    private ResponseDto replconf(String[] command, Client client) {
        if (command.length < 2) {
            return wrongArguments(command[0]);
        }
        String subcommand = command[1].toUpperCase(Locale.ROOT);
        switch (subcommand) {
            case "GETACK": {
                // answer with the offset *before* this GETACK itself is counted
                String[] ack = {"REPLCONF", "ACK", String.valueOf(redisConfig.getMasterReplOffset())};
                return new ResponseDto(respSerializer.respArray(ack));
            }
            case "ACK": {
                if (command.length < 3) {
                    return wrongArguments(command[0]);
                }
                Slave slave = connectionPool.getSlave(client);
                if (slave != null) {
                    connectionPool.slaveAck(slave, Long.parseLong(command[2]));
                }
                return null; // acks are never answered
            }
            case "LISTENING-PORT": {
                if (command.length < 3) {
                    return wrongArguments(command[0]);
                }
                client.setListeningPort(Integer.parseInt(command[2]));
                return new ResponseDto(respSerializer.serializeSimpleString("OK"));
            }
            case "CAPA": {
                for (int i = 2; i < command.length; i++) {
                    client.getCapabilities().add(command[i]);
                }
                return new ResponseDto(respSerializer.serializeSimpleString("OK"));
            }
            default:
                return new ResponseDto(respSerializer.serializeSimpleString("OK"));
        }
    }

    private ResponseDto psync(String[] command, Client client) {
        if (command.length < 3) {
            return wrongArguments(command[0]);
        }
        // Partial resync is not supported, so every PSYNC becomes a full resync.
        // The lock keeps writes from being streamed between the RDB and the moment
        // this replica is registered, so it neither misses nor duplicates a command.
        commandLock.lock();
        try {
            long offset = connectionPool.propagatedBytes();
            byte[] rdb = Base64.getDecoder().decode(EMPTY_RDB_BASE64);

            String fullResync = "+FULLRESYNC " + redisConfig.getMasterReplId() + " " + offset + "\r\n";
            byte[] header = ("$" + rdb.length + "\r\n").getBytes(StandardCharsets.US_ASCII);
            client.send(new ResponseDto(fullResync, concatenate(header, rdb)));

            Slave slave = new Slave(client, offset);
            slave.getCapabilities().addAll(client.getCapabilities());
            connectionPool.addSlave(slave);
        } catch (IOException e) {
            logger.log(Level.WARNING, "PSYNC failed for {0}: {1}", new Object[] {client.describe(), e.getMessage()});
            client.close();
        } finally {
            commandLock.unlock();
        }
        return null; // the reply was already written
    }

    private ResponseDto waitForReplicas(String[] command) throws InterruptedException {
        if (command.length != 3) {
            return wrongArguments(command[0]);
        }
        int required = Integer.parseInt(command[1]);
        long timeoutMillis = Long.parseLong(command[2]);
        if (required < 0 || timeoutMillis < 0) {
            return error("ERR value is not an integer or out of range");
        }

        long target;
        commandLock.lock();
        try {
            if (connectionPool.getSlaveCount() == 0) {
                return new ResponseDto(respSerializer.respInteger(0));
            }
            // Replicas must have processed everything streamed so far.
            target = connectionPool.propagatedBytes();
            int alreadyAcked = connectionPool.countAcked(target);
            if (alreadyAcked >= required) {
                return new ResponseDto(respSerializer.respInteger(alreadyAcked));
            }
            // Ask for fresh acknowledgements. The GETACK itself is part of the stream, so
            // it is sent under the lock to keep its position relative to the writes.
            connectionPool.propagate(respSerializer.encodeCommand(new String[] {"REPLCONF", "GETACK", "*"}));
        } finally {
            commandLock.unlock();
        }

        // Wait outside the lock so other clients (and the acks) are not blocked.
        int acked = connectionPool.awaitAcks(target, required, timeoutMillis);
        return new ResponseDto(respSerializer.respInteger(acked));
    }

    // ------------------------------------------------------------------ helpers

    private ResponseDto error(String message) {
        return new ResponseDto(respSerializer.serializeError(message));
    }

    private ResponseDto wrongArguments(String commandName) {
        return error("ERR wrong number of arguments for '" + commandName.toLowerCase(Locale.ROOT) + "' command");
    }

    private static byte[] concatenate(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}
