# Troubleshooting

This guide covers the Docker, Redpanda, Kafka, and consumer issues encountered while running this project.

Run the commands below from the directory containing `docker-compose.yml` (for example, `~/zed_workspace/stream_an`).

## Start the services

Start Redpanda and the consumer:

```bash
docker compose up -d redpanda consumer
```

Check service status:

```bash
docker compose ps
```

Redpanda should show `healthy`, and the consumer should show `Up`. Follow consumer output with:

```bash
docker compose logs -f consumer
```

To view only recent consumer output (and avoid mistaking earlier errors for current ones):

```bash
docker compose logs --since=30s consumer
```

## Redpanda is unhealthy

The health check must use a command supported by the `rpk` version in the Redpanda image. This project uses:

```yaml
test: ["CMD-SHELL", "rpk cluster health"]
```

An earlier health check used `rpk cluster health --brokers=localhost:9092`; this `rpk` version does not accept the `--brokers` option, so Docker marked Redpanda unhealthy and held back the dependent consumer. After correcting the health check, apply the Compose configuration with:

```bash
docker compose up -d redpanda consumer
```

Check Redpanda logs if it remains unhealthy:

```bash
docker compose logs redpanda
```

## Host port is already allocated

A message such as `Bind for 0.0.0.0:5432 failed: port is already allocated` means another process or container is already using that host port. Find Docker containers publishing a port with:

```bash
docker ps --filter publish=5432 --format 'table {{.Names}}\t{{.Ports}}'
```

For Redpanda's admin port, substitute `9644`:

```bash
docker ps --filter publish=9644 --format 'table {{.Names}}\t{{.Ports}}'
```

Either stop the conflicting container if it is safe to do so, or change the host side of the mapping in `docker-compose.yml`. For example, change `"5432:5432"` to `"5433:5432"`, or `"9644:9644"` to `"19644:9644"`. The left-hand port is on the host; the right-hand port remains the container port. Apply the change by recreating the relevant service:

```bash
docker compose up -d --force-recreate postgres
```

For a Redpanda port change, recreate Redpanda and its dependent consumer:

```bash
docker compose up -d --force-recreate redpanda consumer
```

The PostgreSQL mapping only affects connections from the host. Other Compose services should still connect to PostgreSQL at `postgres:5432`.

## Consumer cannot resolve `redpanda:9092`

The consumer uses `redpanda:9092`, which is the Redpanda service address inside the Compose network. It is not the address to use from the host (the host-side Kafka address is `localhost:19092`).

Test DNS from inside the consumer container:

```bash
docker compose exec consumer python -c "import socket; print(socket.gethostbyname('redpanda'))"
```

If this raises `socket.gaierror`, inspect the containers' network membership:

```bash
docker inspect -f '{{.Name}}: {{json .NetworkSettings.Networks}}' stream_an-consumer-1 stream_an-redpanda-1
docker network inspect stream_an_default --format '{{range .Containers}}{{.Name}} {{.IPv4Address}}{{println}}{{end}}'
```

Both containers should be attached to the same Compose network (`stream_an_default`). If Redpanda is missing from that network, check port conflicts first, then recreate both services from the directory containing the Compose file:

```bash
docker compose up -d --force-recreate redpanda consumer
```

Re-check `docker compose ps` and the network membership. A Redpanda container can be running or report healthy but still be unreachable from the consumer if it is not attached to the same network.

## Topic does not exist

The consumer subscribes to `stream-events`. If Kafka reports `Unknown topic or partition`, create the topic from inside Redpanda:

```bash
docker compose exec redpanda rpk topic create stream-events
```

If the topic already exists, `TOPIC_ALREADY_EXISTS` is harmless. Start the producer from the host in another terminal:

```bash
.venv/bin/python producer.py
```

The producer defaults to `localhost:19092`; the consumer container uses `redpanda:9092`. View received events with:

```bash
docker compose logs -f consumer
```

## Dependency installation

The Compose consumer installs packages from `requirements.txt` when its container starts. The local producer uses `kafka-python`; the consumer script imports `confluent-kafka`. Keep both dependencies in `requirements.txt` when running both scripts. To install them in the local virtual environment:

```bash
pip install -r requirements.txt
```
