#include "TrpMQ.h"

static MQChannel *find_channel(MQBroker *broker, int channel_id)
{
    for (int i = 0; i < broker->n_channels; ++i)
        if (broker->channels[i].id == channel_id)
            return &broker->channels[i];
    return NULL;
}

static MQChannel *get_or_create_channel(MQBroker *broker, int channel_id)
{
    MQChannel *channel = find_channel(broker, channel_id);
    if (channel)
        return channel;
    if (broker->n_channels >= broker->max_channels)
        return NULL;
    channel = &broker->channels[broker->n_channels++];
    memset(channel, 0, sizeof(*channel));
    channel->id = channel_id;
    channel->next_sequence = 1;
    snprintf(channel->name, sizeof(channel->name), "channel-%d", channel_id);
    return channel;
}

int MQ_channel_subscribe(MQBroker *broker, int channel_id, int client_slot)
{
    if (!broker || channel_id < 0 || client_slot < 0)
        return -1;
    MQChannel *channel = get_or_create_channel(broker, channel_id);
    if (!channel)
        return -1;
    for (int i = 0; i < channel->nsubs; ++i)
        if (channel->clients[i] == client_slot)
            return 0;
    if (channel->nsubs == channel->subs_capacity) {
        int next = channel->subs_capacity ? channel->subs_capacity * 2 : 4;
        int *clients = realloc(channel->clients, (size_t)next * sizeof(*clients));
        if (!clients)
            return -1;
        channel->clients = clients;
        channel->subs_capacity = next;
    }
    channel->clients[channel->nsubs++] = client_slot;
    return 0;
}

int MQ_channel_publish(MQBroker *broker, int channel_id, const char *title,
                       const void *body, size_t body_size,
                       uint64_t *sequence_out, long *timestamp_out)
{
    if (!broker || channel_id < 0 || !title || (!body && body_size) ||
        body_size > MQ_MAX_BODY_SIZE || strlen(title) > MQ_TEXT_SIZE - 1)
        return -1;
    MQChannel *channel = get_or_create_channel(broker, channel_id);
    if (!channel)
        return -1;

    MQmessage *message = calloc(1, sizeof(*message));
    MQEvent *event = calloc(1, sizeof(*event));
    if (!message || !event) {
        free(message);
        free(event);
        return -1;
    }
    if (body_size) {
        message->body = malloc(body_size);
        if (!message->body) {
            free(message);
            free(event);
            return -1;
        }
        memcpy(message->body, body, body_size);
    }
    message->body_size = body_size;
    message->timestamp = (long)time(NULL);
    memcpy(message->title, title, strlen(title) + 1);
    event->sequence = channel->next_sequence++;
    event->msg = message;
    event->prev = channel->tail;
    if (channel->tail)
        channel->tail->next = event;
    else
        channel->head = event;
    channel->tail = event;
    if (sequence_out)
        *sequence_out = event->sequence;
    if (timestamp_out)
        *timestamp_out = message->timestamp;
    return 0;
}

void MQ_channel_destroy(MQChannel *channel)
{
    if (!channel)
        return;
    MQEvent *event = channel->head;
    while (event) {
        MQEvent *next = event->next;
        if (event->msg) {
            free(event->msg->body);
            free(event->msg);
        }
        free(event);
        event = next;
    }
    free(channel->clients);
    memset(channel, 0, sizeof(*channel));
}

