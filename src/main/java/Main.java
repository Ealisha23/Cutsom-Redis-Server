import Components.Server.MasterTcpServer;
import Components.Server.RedisConfig;
import Components.Server.SlaveTcpServer;
import Config.AppConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {

    private static final String USAGE = "Usage: --port <port> [--replicaof \"<master-host> <master-port>\"]";

    /** Settings read from the command line. */
    record Options(int port, String masterHost, int masterPort) {
        boolean isReplica() {
            return masterHost != null;
        }
    }

    static Options parseArguments(String[] args) {
        int port = 6379;
        String masterHost = null;
        int masterPort = 0;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--port needs a value");
                    }
                    port = parsePort(args[++i]);
                }
                case "--replicaof" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--replicaof needs a value");
                    }
                    // CodeCrafters passes "<host> <port>" as one argument; also accept two arguments
                    String[] parts = args[++i].trim().split("\\s+");
                    if (parts.length == 1 && i + 1 < args.length) {
                        parts = new String[] {parts[0], args[++i]};
                    }
                    if (parts.length != 2) {
                        throw new IllegalArgumentException("--replicaof expects \"<host> <port>\"");
                    }
                    masterHost = parts[0];
                    masterPort = parsePort(parts[1]);
                }
                default -> System.err.println("Ignoring unknown argument: " + args[i]);
            }
        }
        return new Options(port, masterHost, masterPort);
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("port out of range: " + value);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid port: " + value);
        }
    }

    public static void main(String[] args) {
        Options options;
        try {
            options = parseArguments(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(USAGE);
            System.exit(1);
            return;
        }

        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class)) {
            RedisConfig redisConfig = context.getBean(RedisConfig.class);
            redisConfig.setPort(options.port());

            if (options.isReplica()) {
                redisConfig.setRole(RedisConfig.ROLE_SLAVE);
                redisConfig.setMasterHost(options.masterHost());
                redisConfig.setMasterPort(options.masterPort());
                context.getBean(SlaveTcpServer.class).startServer();
            } else {
                redisConfig.setRole(RedisConfig.ROLE_MASTER);
                context.getBean(MasterTcpServer.class).startServer();
            }
        }
    }
}
