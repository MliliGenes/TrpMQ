#ifndef TRP_MQ_
#define TRP_MQ_

#include <poll.h>       // struct pollfd, poll()
#include <sys/socket.h> // socket(), bind(), listen(), accept(), send(), recv()
#include <netinet/in.h> // struct sockaddr_in, htons(), htonl(), INADDR_ANY
#include <arpa/inet.h>  // inet_pton(), inet_ntoa() — IP string <-> binary conversion
#include <unistd.h>     // close(), read(), write()
#include <string.h>     // memset(), memcpy(), strlen(), strerror()
#include <stdlib.h>     // malloc(), free(), realloc()
#include <stdio.h>      // printf(), perror() — since you're going full verbose
#include <errno.h>      // errno — needed alongside perror()/strerror() for real error handling
#include <stdint.h>     // uint16_t, uint32_t, uint64_t — used in your MQConfig/MQEvent structs
#include <fcntl.h>      // fcntl() — if you ever set sockets non-blocking (O_NONBLOCK)

#define MQ_DEFAULT_IP "0.0.0.0"
#define MQ_DEFAULT_PORT 9000
#define MQ_DEFAULT_BACKLOG 16
#define MQ_DEFAULT_MAX_CLIENTS 1024
#define MQ_MAX_SUBS 32 

typedef struct
{
    char bind_ip[16];
    uint16_t port;

    int backlog;
    int max_clients;
} MQServerConfig;

typedef struct
{
    int listen_fd;

    struct pollfd *fds;
    int nfds;
} MQBroker;

typedef struct
{
    int fd;

    int subs[MQ_MAX_SUBS];
    int nsubs;
} MQClient;

typedef struct
{
    int id;
    char name[120];
    int nsubs;
} MQChannel;

int MQ_init(MQBroker *broker, MQServerConfig *configs);
int MQ_bind(MQBroker *broker, MQServerConfig *configs);
int MQ_listen(MQBroker *broker, MQServerConfig *configs);

int 

int MQ_accpet();

#endif