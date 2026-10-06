#ifndef TRP_MQ_H
#define TRP_MQ_H

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define MQ_DEFAULT_IP "0.0.0.0"
#define MQ_DEFAULT_PORT 9000
#define MQ_DEFAULT_BACKLOG 16
#define MQ_DEFAULT_MAX_CLIENTS 128
#define MQ_DEFAULT_MAX_CHANNELS 64
#define MQ_DEFAULT_MAX_SUBS 64
#define MQ_TEXT_SIZE 255
#define MQ_MAX_BODY_SIZE (1024U * 1024U)
#define MQ_WIRE_HEADER_SIZE 31

typedef struct {
    char bind_ip[16];
    uint16_t port;
    int backlog;
    int max_clients;
    int max_channels;
    int max_subs;
} MQServerConfig;

typedef struct {
    size_t body_size;
    long timestamp;
    char title[MQ_TEXT_SIZE + 1];
    char *body;
} MQmessage;

typedef struct MQEvent {
    uint64_t sequence;
    MQmessage *msg;
    struct MQEvent *next;
    struct MQEvent *prev;
} MQEvent;

typedef struct {
    int id;
    char name[MQ_TEXT_SIZE];
    MQEvent *head;
    MQEvent *tail;
    uint64_t next_sequence;
    int nsubs;
    int *clients;              /* Broker client slot indexes. */
    int subs_capacity;
} MQChannel;

typedef void (*MQMessageCallback)(int channel_id, uint64_t sequence,
                                  const char *title, const void *body,
                                  size_t body_size, void *context);

typedef struct {
    int fd;
    int nsubs;
    int *subs;
    uint64_t *cursors;
    size_t cap;
    int connected;
    int monitor_running;
    unsigned int poll_interval_ms;
    pthread_t monitor_thread;
    pthread_mutex_t io_mutex;
    int mutex_initialized;
    MQMessageCallback on_message;
    void *callback_context;
} MQClient;

typedef struct {
    int listen_fd;
    struct pollfd *fds;         /* fds[0] is the listener; remaining slots map to clients[]. */
    int n_fds;
    int max_clients;
    MQChannel *channels;
    int n_clients;
    MQClient *clients;
    int n_channels;
    int max_channels;
    int max_subs;
} MQBroker;

enum { MQ_CMD_SUBSCRIBE = 1, MQ_CMD_PUBLISH = 2, MQ_CMD_PULL = 3,
       MQ_CMD_EVENT = 4, MQ_CMD_END = 5, MQ_CMD_ERROR = 6 };

int MQ_broker_init(MQBroker *broker, MQServerConfig *configs);
int MQ_pfds_init(MQBroker *broker, MQServerConfig *configs);
int MQ_clients_init(MQBroker *broker, MQServerConfig *configs);
int MQ_channels_init(MQBroker *broker, MQServerConfig *configs);
int MQ_init(MQBroker *broker, MQServerConfig *configs);
int MQ_bind(MQBroker *broker, MQServerConfig *configs);
int MQ_listen(MQBroker *broker, MQServerConfig *configs);
int MQ_accept(MQBroker *broker, MQServerConfig *configs);
int MQ_start(MQBroker *broker, MQServerConfig *configs);
void MQ_broker_destroy(MQBroker *broker);
int MQ_push_fd(MQBroker *broker, MQServerConfig *configs, int socket_fd, short event);
int MQ_set_event(MQBroker *broker, int index, short event);
int MQ_get_event(MQBroker *broker, int index);

int MQ_channel_subscribe(MQBroker *broker, int channel_id, int client_slot);
int MQ_channel_publish(MQBroker *broker, int channel_id, const char *title,
                       const void *body, size_t body_size,
                       uint64_t *sequence_out, long *timestamp_out);
void MQ_channel_destroy(MQChannel *channel);

int MQ_client_connect(MQClient *client, const char *ip, uint16_t port);
int MQ_client_subscribe(MQClient *client, int channel_id);
int MQ_client_publish(MQClient *client, int channel_id, const char *title,
                      const void *body, size_t body_size);
int MQ_client_start_monitor(MQClient *client, unsigned int poll_interval_ms,
                            MQMessageCallback callback, void *context);
void MQ_client_stop_monitor(MQClient *client);
void MQ_client_disconnect(MQClient *client);

/* Shared wire helpers. One frame is a 31-byte network-order header plus payload. */
int MQ_read_full(int fd, void *buffer, size_t length);
int MQ_write_full(int fd, const void *buffer, size_t length);
int MQ_send_frame(int fd, uint8_t type, int32_t channel_id, uint64_t sequence,
                  int64_t timestamp, const char *title, const void *body,
                  uint32_t body_size);
int MQ_read_frame_header(int fd, uint8_t *type, int32_t *channel_id,
                         uint64_t *sequence, int64_t *timestamp,
                         uint16_t *title_size, uint32_t *body_size);

#endif
