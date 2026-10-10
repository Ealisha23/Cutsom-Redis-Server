package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MasterTcpServer extends BaseTcpServer {

    @Autowired
    public MasterTcpServer(RespSerializer respSerializer,
                           CommandHandler commandHandler,
                           RedisConfig redisConfig,
                           ConnectionPool connectionPool) {
        super(respSerializer, commandHandler, redisConfig, connectionPool);
    }

    @Override
    public void startServer() {
        acceptLoop();
    }
}
