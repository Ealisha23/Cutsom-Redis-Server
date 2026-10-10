[![progress-banner](https://backend.codecrafters.io/progress/redis/b7dbc9e6-45a6-4033-97e0-c93c0a469df9)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

A toy Redis server in Java, built for the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).
It speaks the real Redis protocol (RESP), so `redis-cli` and the CodeCrafters tests work against it.

## Features

| Area | Commands / behaviour |
| --- | --- |
| Basics | `PING`, `ECHO`, `INFO replication`, `COMMAND` (empty reply so `redis-cli` can connect) |
| Strings | `SET` (with `EX` / `PX`), `GET`, `DEL`, `INCR` |
| Transactions | `MULTI`, `EXEC`, `DISCARD` (atomic; queued writes are replicated after `EXEC`) |
| Replication | `--replicaof`, handshake (`PING`, `REPLCONF`, `PSYNC`), RDB transfer, command propagation, `REPLCONF GETACK/ACK`, `WAIT` |
| Robustness | pipelining, commands split across TCP packets, UTF-8 values, clean handling of disconnects and malformed input |

Command names and options are case-insensitive. A replica answers `-READONLY` to writes from normal clients.

## Running

Requires JDK 23 and Maven.

```shell
./your_program.sh                                        # master on port 6379
./your_program.sh --port 6380 --replicaof "localhost 6379"   # replica of that master
redis-cli -p 6379 SET foo bar
redis-cli -p 6380 GET foo
```

`--replicaof` takes `"<host> <port>"` as one argument (this is how CodeCrafters passes it).
Bad arguments print a usage message instead of a stack trace.

### Tests

```shell
mvn test
```

* `RespParserTest`, `RespSerializerTest`, `StoreTest`, `CommandHandlerTest` are fast unit tests
  (no Spring context, no sleeping: the store takes an injectable clock).
* `ReplicationIntegrationTest` starts a real master and a real replica on free local ports and talks to
  them over TCP: replication, `WAIT`, transactions, many simultaneous clients, packets split byte by byte, bad input.

## How it is put together

```
Main ──► MasterTcpServer / SlaveTcpServer      (both extend BaseTcpServer)
              │  one virtual thread per client
              ▼
        RespParser (streaming) ──► CommandHandler ──► Store
                                        │
                                        ▼
                                  ConnectionPool ──► replicas
```

* **`RespParser`** reads straight from the socket and takes exactly the bytes a command needs, so it does not
  matter how TCP chops the data. It counts the bytes it consumed, which is what a replica's offset is made of.
* **`CommandHandler`** runs data commands one at a time under a single lock (as real Redis does with its single
  thread). That makes `EXEC` atomic and guarantees replicas see writes in the order they were applied.
  `WAIT` and `REPLCONF ACK` do not hold that lock while waiting.
* **`Store`** only stores values and expiry; it knows nothing about the protocol. Expiry is lazy.
* **`ConnectionPool`** tracks clients and replicas, the replication offset (`master_repl_offset`) and each
  replica's acknowledged offset. `WAIT n ms` sends `REPLCONF GETACK *` and waits until `n` replicas have
  acknowledged the offset the master had when `WAIT` started.
* **`SlaveTcpServer`** performs the handshake, then applies the master's stream; it reconnects with a growing
  delay if the link drops.

## Dockerize

```shell
docker build -t myredis .                 # runs the tests during the build
docker build --build-arg SKIP_TESTS=true -t myredis .   # skip them

docker run -p 6379:6379 myredis

ip route show | grep default | awk '{print $3}'

docker run -p 6480:6480 myredis --port 6480 --replicaof "<ipaddress> 6379"
```

Use the Docker host's IP address for `<ipaddress>`; `localhost` inside a container means the container itself.

### Self Hosted Agent with Docker capabilities

```shell
cd dind

docker build --tag "azp-agent:linux"  .

docker run --privileged -e AZP_URL="https://dev.azure.com/ChaitanyaDSharma" -e AZP_TOKEN="use your own token" -e AZP_POOL="agent" -e AZP_AGENT_NAME="Docker Agent - Linux" --name "azp-agent-linux" azp-agent:linux
```

`azure-pipelines.yml` builds the image (which runs the tests) and pushes it to Docker Hub as
`beelzekamibub/redisjava`, tagged with the build id and `latest`.

### K8s cluster on KIND + Helm

```shell
kind create cluster --config kind/k.yml --name cluster-name

docker build -t beelzekamibub/redisjava:latest .
kind load docker-image beelzekamibub/redisjava:latest --name cluster-name

alias k=kubectl
helm template redis-chart
helm lint redis-chart
helm install redis-app redis-chart
```

The chart (`redis-chart/`) creates a `Deployment` for the master with readiness/liveness probes and a
`NodePort` service on `30007` (mapped to your machine by `kind/k.yml`). Check it with:

```shell
k get pods
./ping.ps1 -Port 30007          # PowerShell: runs PING, SET and GET
```

To also run a read-only replica of the master inside the cluster:

```shell
helm upgrade --install redis-app redis-chart --set replica.enabled=true
```

Clean up with:

```shell
helm uninstall redis-app
kind delete cluster --name cluster-name
```
