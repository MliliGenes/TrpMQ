#include "TrpMQ.h"

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

    return 0;
}