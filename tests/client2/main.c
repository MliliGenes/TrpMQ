#include "TrpMQ.h"

#include <signal.h>

static volatile sig_atomic_t keep_running = 1;

static void handle_signal(int signal_number)
{
    (void)signal_number;
    keep_running = 0;
}

static void print_user_created(int channel_id, uint64_t sequence,
                               const char *title, const void *body,
                               size_t body_size, void *context)
{
    (void)context;
    printf("\n[client2] channel=%d sequence=%llu event=%s\nuser buffer: ",
           channel_id, (unsigned long long)sequence, title);
    if (body_size)
        fwrite(body, 1, body_size, stdout);
    putchar('\n');
    fflush(stdout);
}

int main(int argc, char **argv)
{
    if (argc != 4) {
        fprintf(stderr, "usage: %s <broker-ip> <port> <channel-id>\n", argv[0]);
        return 2;
    }
    char *end = NULL;
    long port = strtol(argv[2], &end, 10);
    if (!end || *end || port < 1 || port > 65535) {
        fprintf(stderr, "invalid port\n");
        return 2;
    }
    long channel_value = strtol(argv[3], &end, 10);
    if (!end || *end || channel_value < 0 || channel_value > INT32_MAX) {
        fprintf(stderr, "invalid channel ID\n");
        return 2;
    }

    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = handle_signal;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);

    MQClient client;
    if (MQ_client_connect(&client, argv[1], (uint16_t)port) < 0) {
        perror("connect");
        return 1;
    }
    if (MQ_client_subscribe(&client, (int)channel_value) < 0) {
        fprintf(stderr, "subscribe failed for channel %ld\n", channel_value);
        MQ_client_disconnect(&client);
        return 1;
    }
    if (MQ_client_start_monitor(&client, 100, print_user_created, NULL) < 0) {
        fprintf(stderr, "could not start pthread monitor\n");
        MQ_client_disconnect(&client);
        return 1;
    }

    printf("[client2] listening on channel %ld; press Ctrl-C to stop\n",
           channel_value);
    while (keep_running) {
        struct timespec delay = {0, 100000000L};
        nanosleep(&delay, NULL);
    }
    MQ_client_disconnect(&client);
    return 0;
}
