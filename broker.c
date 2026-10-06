#include "TrpMQ.h"

#define MQ_WIRE_MAGIC "TMQ1"

static void put_u64(unsigned char *out, uint64_t value)
{
    for (int i = 7; i >= 0; --i) {
        out[i] = (unsigned char)(value & 0xffU);
        value >>= 8;
    }
}

static uint64_t get_u64(const unsigned char *in)
{
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i)
        value = (value << 8) | in[i];
    return value;
}

int MQ_read_full(int fd, void *buffer, size_t length)
{
    unsigned char *at = buffer;
    size_t done = 0;
    while (done < length) {
        ssize_t n = recv(fd, at + done, length - done, 0);
        if (n == 0)
            return 0;
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        done += (size_t)n;
    }
    return 1;
}

int MQ_write_full(int fd, const void *buffer, size_t length)
{
    const unsigned char *at = buffer;
    size_t done = 0;
    while (done < length) {
#ifdef MSG_NOSIGNAL
        ssize_t n = send(fd, at + done, length - done, MSG_NOSIGNAL);
#else
        ssize_t n = send(fd, at + done, length - done, 0);
#endif
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        done += (size_t)n;
    }
    return 0;
}

int MQ_send_frame(int fd, uint8_t type, int32_t channel_id, uint64_t sequence,
                  int64_t timestamp, const char *title, const void *body,
                  uint32_t body_size)
{
    size_t title_size = title ? strlen(title) : 0;
    if (title_size > MQ_TEXT_SIZE - 1 || body_size > MQ_MAX_BODY_SIZE ||
        (!body && body_size))
        return -1;
    unsigned char header[MQ_WIRE_HEADER_SIZE] = {0};
    memcpy(header, MQ_WIRE_MAGIC, 4);
    header[4] = type;
    uint32_t cid = htonl((uint32_t)channel_id);
    memcpy(header + 5, &cid, sizeof(cid));
    put_u64(header + 9, sequence);
    put_u64(header + 17, (uint64_t)timestamp);
    uint16_t title_len = htons((uint16_t)title_size);
    memcpy(header + 25, &title_len, sizeof(title_len));
    uint32_t payload_len = htonl(body_size);
    memcpy(header + 27, &payload_len, sizeof(payload_len));
    if (MQ_write_full(fd, header, sizeof(header)) < 0)
        return -1;
    if (title_size && MQ_write_full(fd, title, title_size) < 0)
        return -1;
    if (body_size && MQ_write_full(fd, body, body_size) < 0)
        return -1;
    return 0;
}

int MQ_read_frame_header(int fd, uint8_t *type, int32_t *channel_id,
                         uint64_t *sequence, int64_t *timestamp,
                         uint16_t *title_size, uint32_t *body_size)
{
    unsigned char header[MQ_WIRE_HEADER_SIZE];
    int result = MQ_read_full(fd, header, sizeof(header));
    if (result != 1)
        return result;
    if (memcmp(header, MQ_WIRE_MAGIC, 4) != 0)
        return -1;
    uint32_t cid;
    uint16_t title_len;
    uint32_t payload_len;
    memcpy(&cid, header + 5, sizeof(cid));
    memcpy(&title_len, header + 25, sizeof(title_len));
    memcpy(&payload_len, header + 27, sizeof(payload_len));
    *type = header[4];
    *channel_id = (int32_t)ntohl(cid);
    *sequence = get_u64(header + 9);
    *timestamp = (int64_t)get_u64(header + 17);
    *title_size = ntohs(title_len);
    *body_size = ntohl(payload_len);
    if (*title_size > MQ_TEXT_SIZE - 1 || *body_size > MQ_MAX_BODY_SIZE)
        return -1;
    return 1;
}

static int client_slot_for_fd(MQBroker *broker, int fd)
{
    for (int i = 0; i < broker->max_clients; ++i)
        if (broker->clients[i].fd == fd)
            return i;
    return -1;
}

static int client_has_sub(MQClient *client, int channel_id)
{
    for (int i = 0; i < client->nsubs; ++i)
        if (client->subs[i] == channel_id)
            return i;
    return -1;
}

static int add_client_sub(MQBroker *broker, int slot, int channel_id)
{
    MQClient *client = &broker->clients[slot];
    if (client_has_sub(client, channel_id) >= 0)
        return 0;
    if (client->nsubs >= broker->max_subs)
        return -1;
    client->subs[client->nsubs] = channel_id;
    client->cursors[client->nsubs] = 0;
    ++client->nsubs;
    if (MQ_channel_subscribe(broker, channel_id, slot) < 0) {
        --client->nsubs;
        return -1;
    }
    return 0;
}

static int send_error(int fd, int channel_id)
{
    static const char error_text[] = "invalid command, subscription, or limit";
    return MQ_send_frame(fd, MQ_CMD_ERROR, channel_id, 0, 0, "error",
                         error_text, (uint32_t)(sizeof(error_text) - 1));
}

static int handle_client_frame(MQBroker *broker, int slot)
{
    MQClient *client = &broker->clients[slot];
    uint8_t type;
    int32_t channel_id;
    uint64_t sequence;
    int64_t timestamp;
    uint16_t title_size;
    uint32_t body_size;
    int status = MQ_read_frame_header(client->fd, &type, &channel_id, &sequence,
                                      &timestamp, &title_size, &body_size);
    (void)timestamp;
    if (status != 1)
        return -1;
    char title[MQ_TEXT_SIZE] = {0};
    if (title_size && MQ_read_full(client->fd, title, title_size) != 1)
        return -1;
    void *body = NULL;
    if (body_size) {
        body = malloc(body_size);
        if (!body || MQ_read_full(client->fd, body, body_size) != 1) {
            free(body);
            return -1;
        }
    }

    int rc = 0;
    if (type == MQ_CMD_SUBSCRIBE && title_size == 0 && body_size == 0) {
        rc = add_client_sub(broker, slot, channel_id);
        if (rc == 0)
            rc = MQ_send_frame(client->fd, MQ_CMD_END, channel_id, 0, 0, NULL, NULL, 0);
    } else if (type == MQ_CMD_PUBLISH) {
        uint64_t published_sequence;
        long published_at;
        rc = MQ_channel_publish(broker, channel_id, title, body, body_size,
                                &published_sequence, &published_at);
        if (rc == 0)
            rc = MQ_send_frame(client->fd, MQ_CMD_END, channel_id,
                               published_sequence, published_at, NULL, NULL, 0);
    } else if (type == MQ_CMD_PULL && title_size == 0 && body_size == 0) {
        int sub = client_has_sub(client, channel_id);
        if (sub < 0) {
            rc = -1;
        } else {
            MQChannel *channel = NULL;
            for (int i = 0; i < broker->n_channels; ++i)
                if (broker->channels[i].id == channel_id) {
                    channel = &broker->channels[i];
                    break;
                }
            if (!channel)
                rc = -1;
            else {
                for (MQEvent *event = channel->head; event && rc == 0; event = event->next) {
                    if (event->sequence <= sequence)
                        continue;
                    rc = MQ_send_frame(client->fd, MQ_CMD_EVENT, channel_id,
                                       event->sequence, event->msg->timestamp,
                                       event->msg->title, event->msg->body,
                                       (uint32_t)event->msg->body_size);
                }
                if (rc == 0)
                    rc = MQ_send_frame(client->fd, MQ_CMD_END, channel_id, 0, 0,
                                       NULL, NULL, 0);
            }
        }
    } else {
        rc = -1;
    }
    free(body);
    if (rc < 0) {
        send_error(client->fd, channel_id);
        return -1;
    }
    return 0;
}

int MQ_push_fd(MQBroker *broker, MQServerConfig *configs, int socket_fd, short event)
{
    if (!broker || !configs || socket_fd < 0 || !event ||
        broker->n_clients >= configs->max_clients)
        return -1;
    for (int slot = 0; slot < broker->max_clients; ++slot) {
        if (broker->clients[slot].fd >= 0)
            continue;
        broker->clients[slot].fd = socket_fd;
        broker->fds[slot + 1].fd = socket_fd;
        broker->fds[slot + 1].events = event;
        broker->fds[slot + 1].revents = 0;
        ++broker->n_clients;
        if (broker->n_fds < slot + 2)
            broker->n_fds = slot + 2;
        return 0;
    }
    return -1;
}

int MQ_get_event(MQBroker *broker, int index)
{
    if (!broker || index < 0 || index >= broker->n_fds)
        return -1;
    return broker->fds[index].revents;
}

int MQ_set_event(MQBroker *broker, int index, short event)
{
    if (!broker || index < 0 || index >= broker->n_fds || !event)
        return -1;
    broker->fds[index].events = event;
    return 0;
}

int MQ_broker_init(MQBroker *broker, MQServerConfig *configs)
{
    if (!broker || !configs || configs->max_clients <= 0 ||
        configs->max_channels <= 0 || configs->max_subs <= 0)
        return -1;
    memset(broker, 0, sizeof(*broker));
    broker->listen_fd = -1;
    broker->max_clients = configs->max_clients;
    broker->max_channels = configs->max_channels;
    broker->max_subs = configs->max_subs;
    return 0;
}

int MQ_pfds_init(MQBroker *broker, MQServerConfig *configs)
{
    broker->fds = calloc((size_t)configs->max_clients + 1, sizeof(*broker->fds));
    if (!broker->fds)
        return -1;
    broker->n_fds = 1;
    for (int i = 0; i <= configs->max_clients; ++i)
        broker->fds[i].fd = -1;
    return 0;
}

int MQ_clients_init(MQBroker *broker, MQServerConfig *configs)
{
    broker->clients = calloc((size_t)configs->max_clients, sizeof(*broker->clients));
    if (!broker->clients)
        return -1;
    for (int i = 0; i < configs->max_clients; ++i)
        broker->clients[i].fd = -1;
    for (int i = 0; i < configs->max_clients; ++i) {
        MQClient *client = &broker->clients[i];
        client->cap = (size_t)configs->max_subs;
        client->subs = calloc(client->cap, sizeof(*client->subs));
        client->cursors = calloc(client->cap, sizeof(*client->cursors));
        if (!client->subs || !client->cursors)
            return -1;
    }
    return 0;
}

int MQ_channels_init(MQBroker *broker, MQServerConfig *configs)
{
    broker->channels = calloc((size_t)configs->max_channels, sizeof(*broker->channels));
    return broker->channels ? 0 : -1;
}

int MQ_init(MQBroker *broker, MQServerConfig *configs)
{
    if (MQ_broker_init(broker, configs) < 0 || MQ_pfds_init(broker, configs) < 0 ||
        MQ_clients_init(broker, configs) < 0 || MQ_channels_init(broker, configs) < 0)
        return -1;
    broker->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (broker->listen_fd < 0)
        return -1;
    int yes = 1;
    if (setsockopt(broker->listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
        return -1;
    return 0;
}

int MQ_bind(MQBroker *broker, MQServerConfig *configs)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(configs->port);
    if (inet_pton(AF_INET, configs->bind_ip, &addr.sin_addr) != 1)
        return -1;
    return bind(broker->listen_fd, (struct sockaddr *)&addr, sizeof(addr));
}

int MQ_listen(MQBroker *broker, MQServerConfig *configs)
{
    if (listen(broker->listen_fd, configs->backlog) < 0)
        return -1;
    broker->fds[0].fd = broker->listen_fd;
    broker->fds[0].events = POLLIN;
    return 0;
}

int MQ_accept(MQBroker *broker, MQServerConfig *configs)
{
    struct sockaddr_in peer;
    socklen_t peer_size = sizeof(peer);
    int fd = accept(broker->listen_fd, (struct sockaddr *)&peer, &peer_size);
    if (fd < 0)
        return -1;
    if (MQ_push_fd(broker, configs, fd, POLLIN) < 0) {
        close(fd);
        return -1;
    }
    char ip[INET_ADDRSTRLEN] = "?";
    inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
    printf("client connected: %s:%u\n", ip, (unsigned)ntohs(peer.sin_port));
    return 0;
}

static void remove_client(MQBroker *broker, int slot)
{
    MQClient *client = &broker->clients[slot];
    if (client->fd >= 0)
        close(client->fd);
    for (int i = 0; i < broker->n_channels; ++i) {
        MQChannel *channel = &broker->channels[i];
        for (int j = 0; j < channel->nsubs;) {
            if (channel->clients[j] == slot) {
                memmove(&channel->clients[j], &channel->clients[j + 1],
                        (size_t)(channel->nsubs - j - 1) * sizeof(*channel->clients));
                --channel->nsubs;
            } else {
                ++j;
            }
        }
    }
    memset(client->subs, 0, client->cap * sizeof(*client->subs));
    memset(client->cursors, 0, client->cap * sizeof(*client->cursors));
    client->fd = -1;
    client->nsubs = 0;
    broker->fds[slot + 1].fd = -1;
    broker->fds[slot + 1].events = 0;
    broker->fds[slot + 1].revents = 0;
    --broker->n_clients;
    while (broker->n_fds > 1 && broker->fds[broker->n_fds - 1].fd < 0)
        --broker->n_fds;
}

int MQ_start(MQBroker *broker, MQServerConfig *configs)
{
    if (!broker || !configs || broker->listen_fd < 0 || !broker->fds)
        return -1;
    printf("TrpMQ listening on %s:%u\n", configs->bind_ip, (unsigned)configs->port);
    while (1) {
        int ready = poll(broker->fds, (nfds_t)broker->n_fds, -1);
        if (ready < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (broker->fds[0].revents & POLLIN)
            (void)MQ_accept(broker, configs);
        for (int i = 1; i < broker->n_fds; ++i) {
            if (!broker->fds[i].revents)
                continue;
            int slot = client_slot_for_fd(broker, broker->fds[i].fd);
            if (slot < 0)
                continue;
            if (broker->fds[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                remove_client(broker, slot);
                continue;
            }
            if ((broker->fds[i].revents & POLLIN) &&
                handle_client_frame(broker, slot) < 0)
                remove_client(broker, slot);
        }
    }
}

void MQ_broker_destroy(MQBroker *broker)
{
    if (!broker)
        return;
    if (broker->listen_fd >= 0)
        close(broker->listen_fd);
    for (int i = 0; i < broker->max_clients; ++i) {
        if (broker->clients && broker->clients[i].fd >= 0)
            close(broker->clients[i].fd);
        if (broker->clients) {
            free(broker->clients[i].subs);
            free(broker->clients[i].cursors);
        }
    }
    for (int i = 0; i < broker->n_channels; ++i)
        MQ_channel_destroy(&broker->channels[i]);
    free(broker->clients);
    free(broker->channels);
    free(broker->fds);
    memset(broker, 0, sizeof(*broker));
    broker->listen_fd = -1;
}
