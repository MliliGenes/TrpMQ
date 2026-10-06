# TrpMQ

A small TCP publish/subscribe broker written in C. The broker keeps an in-memory
message history per channel. Each connected subscriber keeps its own sequence
cursor, so pulling from one client does not consume messages for another.

## Build and run

```sh
make
./mq_server [port]                 # defaults to 0.0.0.0:9000
./mq_client 127.0.0.1 9000 1 2    # subscribe to channel IDs 1 and 2
```

In the client, publish using `<channel-id> <title>|<message>`. Ctrl-D exits.
The pthread monitor polls every subscribed channel every 250 ms and calls the
registered message callback for each new event.

## Wire protocol

Each frame is a 31-byte header followed by `title_size` bytes and then
`body_size` bytes. The header contains the `TMQ1` magic, command type, channel
ID, sequence cursor, timestamp, title length, and body length in network byte
order. Commands are `SUBSCRIBE`, `PUBLISH`, and `PULL`; responses are `EVENT`,
`END`, or `ERROR`. Pull responses contain all events with a sequence greater
than the cursor, followed by `END`.

This is a learning broker: messages are held in memory without retention or
authentication, and a client reconnect starts with cursor zero.
