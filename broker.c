#include "TrpMQ.h"

int MQ_push_fd(MQBroker *broker, MQServerConfig *configs, int socket_fd, short event)
{
    if (event != POLLIN && event != POLLOUT && event != (POLLIN | POLLOUT))
        return -1;

    if (broker->n_clients >= configs->max_clients + 1)
        return -1;

    broker->fds[broker->n_clients].fd = socket_fd;
    broker->fds[broker->n_clients].events = event;

    broker->n_clients++;

    return 0;
}

int MQ_get_event(MQBroker *broker, int idx)
{
    if (idx < 0 || idx >= broker->n_clients)
        return -1;

    return (int)broker->fds[idx + 1].revents;
}

int MQ_set_event(MQBroker *broker, int index, short event)
{
    if (index < 0 || index >= broker->n_clients + 1)
        return -1;

    if (event != POLLIN && event != POLLOUT && event != (POLLIN | POLLOUT))
        return -1;

    broker->fds[index].events = event;

    return 0;
}

int MQ_broker_init(MQBroker *broker, MQServerConfig *configs)
{
    broker->listen_fd = -1;
    broker->fds = NULL;
    broker->channels = NULL;
    broker->clients = NULL;
    broker->n_clients = 0;
    broker->n_channels = 0;

    return 0;
}

int MQ_pfds_init(MQBroker *broker, MQServerConfig *configs)
{
    struct pollfd *pfds = (struct pollfd *)calloc(configs->max_clients + 1, sizeof(struct pollfd));

    if (!pfds)
        return -1;

    broker->fds = (struct pollfd *)pfds;
    return 0;
}

int MQ_clients_init(MQBroker *broker, MQServerConfig *configs)
{
    MQClient *clients = (MQClient *)calloc(configs->max_clients, sizeof(MQClient));

    if (!clients)
        return -1;

    broker->clients = (MQClient *)clients;
    return 0;
}

int MQ_channels_init(MQBroker *broker, MQServerConfig *configs)
{
    MQChannel *channels = (MQChannel *)calloc(configs->max_channels, sizeof(MQChannel));

    if (!channels)
        return -1;

    broker->channels = (MQChannel *)channels;
    return 0;
}

int MQ_init(MQBroker *broker, MQServerConfig *configs)
{
    int sfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sfd < 0)
        return -1;

    broker->listen_fd = sfd;

    return 0;
}

int MQ_bind(MQBroker *broker, MQServerConfig *configs)
{
    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(configs->port);

    if (inet_pton(AF_INET, configs->bind_ip, &addr.sin_addr) <= 0)
        return -1;

    if (bind(broker->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        return -1;

    return 0;
}

int MQ_listen(MQBroker *broker, MQServerConfig *configs)
{
    if (listen(broker->listen_fd, configs->backlog) < 0)
        return -1;

    MQ_push_fd(broker, configs, broker->listen_fd, POLLIN);
    return 0;
}