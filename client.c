#include "TrpMQ.h"

typedef struct MQPending {
    int channel_id;
    uint64_t sequence;
    char title[MQ_TEXT_SIZE];
    void *body;
    uint32_t body_size;
    struct MQPending *next;
} MQPending;

static int read_reply(MQClient *client, int expected_channel, int *finished,
                      uint64_t *latest_sequence)
{
    MQPending *head = NULL;
    MQPending **tail = &head;
    int result = 0;
    *finished = 0;
    if (latest_sequence)
        *latest_sequence = 0;
    while (!*finished) {
        uint8_t type;
        int32_t channel_id;
        uint64_t sequence;
        int64_t timestamp;
        uint16_t title_size;
        uint32_t body_size;
        int status = MQ_read_frame_header(client->fd, &type, &channel_id,
                                          &sequence, &timestamp,
                                          &title_size, &body_size);
        (void)timestamp;
        if (status != 1) {
            result = -1;
            break;
        }
        char title[MQ_TEXT_SIZE] = {0};
        if (title_size && MQ_read_full(client->fd, title, title_size) != 1) {
            result = -1;
            break;
        }
        if (type == MQ_CMD_END && channel_id == expected_channel) {
            *finished = 1;
            continue;
        }
        if (type == MQ_CMD_ERROR) {
            char error[MQ_MAX_BODY_SIZE + 1];
            if (body_size && MQ_read_full(client->fd, error, body_size) != 1) {
                result = -1;
                break;
            }
            error[body_size] = '\0';
            fprintf(stderr, "TrpMQ server error: %s\n", error);
            result = -1;
            break;
        }
        if (type != MQ_CMD_EVENT || channel_id != expected_channel) {
            result = -1;
            break;
        }
        MQPending *pending = calloc(1, sizeof(*pending));
        if (!pending) {
            result = -1;
            break;
        }
        pending->channel_id = channel_id;
        pending->sequence = sequence;
        if (latest_sequence && sequence > *latest_sequence)
            *latest_sequence = sequence;
        memcpy(pending->title, title, (size_t)title_size);
        pending->body_size = body_size;
        if (body_size) {
            pending->body = malloc(body_size);
            if (!pending->body || MQ_read_full(client->fd, pending->body, body_size) != 1) {
                free(pending->body);
                free(pending);
                result = -1;
                break;
            }
        }
        *tail = pending;
        tail = (MQPending **)&pending->next;
    }

    if (result == 0 && *finished && client->on_message) {
        for (MQPending *item = head; item; item = item->next)
            client->on_message(item->channel_id, item->sequence, item->title,
                               item->body, item->body_size,
                               client->callback_context);
    }
    while (head) {
        MQPending *next = head->next;
        free(head->body);
        free(head);
        head = next;
    }
    return result;
}

int MQ_client_connect(MQClient *client, const char *ip, uint16_t port)
{
    if (!client || !ip)
        return -1;
    memset(client, 0, sizeof(*client));
    client->fd = -1;
    if (pthread_mutex_init(&client->io_mutex, NULL) != 0)
        return -1;
    client->mutex_initialized = 1;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        goto fail;
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &address.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        close(fd);
        goto fail;
    }
    client->fd = fd;
    client->connected = 1;
    client->cap = 4;
    client->subs = calloc(client->cap, sizeof(*client->subs));
    client->cursors = calloc(client->cap, sizeof(*client->cursors));
    if (!client->subs || !client->cursors)
        goto fail;
    return 0;
fail:
    free(client->subs);
    free(client->cursors);
    client->subs = NULL;
    client->cursors = NULL;
    if (client->fd >= 0)
        close(client->fd);
    client->fd = -1;
    client->connected = 0;
    pthread_mutex_destroy(&client->io_mutex);
    client->mutex_initialized = 0;
    return -1;
}

int MQ_client_subscribe(MQClient *client, int channel_id)
{
    if (!client || !client->connected || channel_id < 0)
        return -1;
    pthread_mutex_lock(&client->io_mutex);
    for (int i = 0; i < client->nsubs; ++i) {
        if (client->subs[i] == channel_id) {
            pthread_mutex_unlock(&client->io_mutex);
            return 0;
        }
    }
    if ((size_t)client->nsubs == client->cap) {
        size_t next = client->cap * 2;
        int *subs = realloc(client->subs, next * sizeof(*subs));
        if (!subs) {
            pthread_mutex_unlock(&client->io_mutex);
            return -1;
        }
        client->subs = subs;
        uint64_t *cursors = realloc(client->cursors, next * sizeof(*cursors));
        if (!cursors) {
            pthread_mutex_unlock(&client->io_mutex);
            return -1;
        }
        client->cursors = cursors;
        client->cap = next;
    }
    int rc = MQ_send_frame(client->fd, MQ_CMD_SUBSCRIBE, channel_id, 0, 0,
                           NULL, NULL, 0);
    int finished = 0;
    if (rc == 0)
        rc = read_reply(client, channel_id, &finished, NULL);
    if (rc == 0 && finished) {
        client->subs[client->nsubs] = channel_id;
        client->cursors[client->nsubs] = 0;
        ++client->nsubs;
    } else {
        rc = -1;
    }
    pthread_mutex_unlock(&client->io_mutex);
    return rc;
}

int MQ_client_publish(MQClient *client, int channel_id, const char *title,
                      const void *body, size_t body_size)
{
    if (!client || !client->connected || !title || body_size > MQ_MAX_BODY_SIZE)
        return -1;
    pthread_mutex_lock(&client->io_mutex);
    int rc = MQ_send_frame(client->fd, MQ_CMD_PUBLISH, channel_id, 0, 0,
                           title, body, (uint32_t)body_size);
    int finished = 0;
    if (rc == 0)
        rc = read_reply(client, channel_id, &finished, NULL);
    pthread_mutex_unlock(&client->io_mutex);
    return (rc == 0 && finished) ? 0 : -1;
}

static void *monitor_channels(void *arg)
{
    MQClient *client = arg;
    for (;;) {
        pthread_mutex_lock(&client->io_mutex);
        if (!client->monitor_running || !client->connected) {
            pthread_mutex_unlock(&client->io_mutex);
            break;
        }
        int failed = 0;
        for (int i = 0; i < client->nsubs && client->monitor_running; ++i) {
            int channel_id = client->subs[i];
            int rc = MQ_send_frame(client->fd, MQ_CMD_PULL, channel_id,
                                   client->cursors[i], 0, NULL, NULL, 0);
            int finished = 0;
            uint64_t latest_sequence = 0;
            if (rc == 0)
                rc = read_reply(client, channel_id, &finished, &latest_sequence);
            if (rc < 0 || !finished) {
                failed = 1;
                break;
            }
            client->cursors[i] = latest_sequence > client->cursors[i]
                                     ? latest_sequence : client->cursors[i];
        }
        if (failed)
            client->connected = 0;
        unsigned int interval = client->poll_interval_ms;
        pthread_mutex_unlock(&client->io_mutex);
        if (failed)
            break;
        struct timespec delay;
        delay.tv_sec = interval / 1000U;
        delay.tv_nsec = (long)(interval % 1000U) * 1000000L;
        while (nanosleep(&delay, &delay) < 0 && errno == EINTR) {}
    }
    return NULL;
}

int MQ_client_start_monitor(MQClient *client, unsigned int poll_interval_ms,
                            MQMessageCallback callback, void *context)
{
    if (!client || !client->connected || !client->mutex_initialized ||
        client->monitor_running)
        return -1;
    client->poll_interval_ms = poll_interval_ms ? poll_interval_ms : 500;
    client->on_message = callback;
    client->callback_context = context;
    client->monitor_running = 1;
    if (pthread_create(&client->monitor_thread, NULL, monitor_channels, client) != 0) {
        client->monitor_running = 0;
        return -1;
    }
    return 0;
}

void MQ_client_stop_monitor(MQClient *client)
{
    if (!client || !client->monitor_running)
        return;
    pthread_mutex_lock(&client->io_mutex);
    client->monitor_running = 0;
    pthread_mutex_unlock(&client->io_mutex);
    pthread_join(client->monitor_thread, NULL);
}

void MQ_client_disconnect(MQClient *client)
{
    if (!client)
        return;
    MQ_client_stop_monitor(client);
    if (client->fd >= 0) {
        shutdown(client->fd, SHUT_RDWR);
        close(client->fd);
    }
    free(client->subs);
    free(client->cursors);
    client->subs = NULL;
    client->cursors = NULL;
    client->fd = -1;
    client->connected = 0;
    if (client->mutex_initialized) {
        pthread_mutex_destroy(&client->io_mutex);
        client->mutex_initialized = 0;
    }
}
