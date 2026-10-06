#include "TrpMQ.h"

int main(int argc, char **argv)
{
    MQServerConfig config = {0};
    snprintf(config.bind_ip, sizeof(config.bind_ip), "%s", MQ_DEFAULT_IP);
    config.port = MQ_DEFAULT_PORT;
    config.backlog = MQ_DEFAULT_BACKLOG;
    config.max_clients = MQ_DEFAULT_MAX_CLIENTS;
    config.max_channels = MQ_DEFAULT_MAX_CHANNELS;
    config.max_subs = MQ_DEFAULT_MAX_SUBS;
    if (argc > 1) {
        char *end = NULL;
        long port = strtol(argv[1], &end, 10);
        if (!end || *end || port < 1 || port > 65535) {
            fprintf(stderr, "usage: %s [port]\n", argv[0]);
            return 2;
        }
        config.port = (uint16_t)port;
    }
    MQBroker broker;
    if (MQ_init(&broker, &config) < 0 || MQ_bind(&broker, &config) < 0 ||
        MQ_listen(&broker, &config) < 0) {
        perror("TrpMQ server startup");
        MQ_broker_destroy(&broker);
        return 1;
    }
    int result = MQ_start(&broker, &config);
    if (result < 0)
        perror("TrpMQ server");
    MQ_broker_destroy(&broker);
    return result < 0;
}
