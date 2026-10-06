#include "TrpMQ.h"

static void print_message(int channel_id, uint64_t sequence, const char *title,
                          const void *body, size_t body_size, void *context)
{
    (void)context;
    printf("\n[channel %d #%llu] %s: ", channel_id,
           (unsigned long long)sequence, title);
    if (body_size)
        fwrite(body, 1, body_size, stdout);
    putchar('\n');
    fflush(stdout);
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "usage: %s <ip> <port> <channel-id> [channel-id ...]\n",
                argv[0]);
        return 2;
    }
    char *end = NULL;
    long port = strtol(argv[2], &end, 10);
    if (!end || *end || port < 1 || port > 65535) {
        fprintf(stderr, "invalid port\n");
        return 2;
    }
    MQClient client;
    if (MQ_client_connect(&client, argv[1], (uint16_t)port) < 0) {
        perror("connect");
        return 1;
    }
    for (int i = 3; i < argc; ++i) {
        long id = strtol(argv[i], &end, 10);
        if (!end || *end || id < 0 || id > INT32_MAX ||
            MQ_client_subscribe(&client, (int)id) < 0) {
            fprintf(stderr, "invalid or rejected channel id: %s\n", argv[i]);
            MQ_client_disconnect(&client);
            return 1;
        }
    }
    if (MQ_client_start_monitor(&client, 250, print_message, NULL) < 0) {
        fprintf(stderr, "could not start channel monitor\n");
        MQ_client_disconnect(&client);
        return 1;
    }
    printf("Subscribed. Publish with: <channel-id> <title>|<message>. Ctrl-D exits.\n");
    char *line = NULL;
    size_t capacity = 0;
    while (getline(&line, &capacity, stdin) >= 0) {
        char *separator = strchr(line, ' ');
        if (!separator)
            continue;
        *separator++ = '\0';
        char *end_id = NULL;
        long id = strtol(line, &end_id, 10);
        char *bar = strchr(separator, '|');
        if (!end_id || *end_id || id < 0 || id > INT32_MAX || !bar)
            continue;
        *bar++ = '\0';
        size_t body_size = strlen(bar);
        if (body_size && bar[body_size - 1] == '\n')
            --body_size;
        if (MQ_client_publish(&client, (int)id, separator, bar, body_size) < 0)
            fprintf(stderr, "publish failed\n");
    }
    free(line);
    MQ_client_disconnect(&client);
    return 0;
}
