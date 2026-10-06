#include "TrpMQ.h"

#include <stdatomic.h>

static atomic_int notification_received = 0;

static void print_user_created(int channel_id, uint64_t sequence,
                               const char *title, const void *body,
                               size_t body_size, void *context)
{
    (void)context;
    printf("\n[observer client] channel=%d sequence=%llu event=%s\n",
           channel_id, (unsigned long long)sequence, title);
    printf("flattened user buffer: ");
    if (body_size)
        fwrite(body, 1, body_size, stdout);
    putchar('\n');
    fflush(stdout);
    atomic_store(&notification_received, 1);
}

static int read_form_field(const char *label, char *field, size_t capacity)
{
    printf("%s: ", label);
    fflush(stdout);
    if (!fgets(field, (int)capacity, stdin))
        return -1;
    size_t length = strlen(field);
    if (length && field[length - 1] == '\n') {
        field[--length] = '\0';
    } else if (!feof(stdin)) {
        int ch;
        while ((ch = getchar()) != '\n' && ch != EOF) {}
        fprintf(stderr, "Input is too long (maximum %zu characters).\n", capacity - 1);
        return -1;
    }
    if (length == 0) {
        fprintf(stderr, "%s cannot be empty.\n", label);
        return -1;
    }
    return 0;
}

static int append_url_encoded(char *output, size_t capacity, size_t *used,
                              const char *value)
{
    static const char hex[] = "0123456789ABCDEF";
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p) {
        unsigned char ch = *p;
        int plain = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                    (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' ||
                    ch == '.' || ch == '~';
        size_t need = plain ? 1 : 3;
        if (*used + need >= capacity)
            return -1;
        if (plain) {
            output[(*used)++] = (char)ch;
        } else {
            output[(*used)++] = '%';
            output[(*used)++] = hex[ch >> 4];
            output[(*used)++] = hex[ch & 0x0f];
        }
    }
    output[*used] = '\0';
    return 0;
}

static int flatten_user(const char *username, const char *email,
                        const char *display_name, char *buffer,
                        size_t buffer_size)
{
    static const char prefix[] = "event=user_created&username=";
    static const char email_key[] = "&email=";
    static const char display_key[] = "&display_name=";
    size_t used = sizeof(prefix) - 1;
    if (used >= buffer_size)
        return -1;
    memcpy(buffer, prefix, used + 1);
    if (append_url_encoded(buffer, buffer_size, &used, username) < 0 ||
        used + sizeof(email_key) > buffer_size)
        return -1;
    memcpy(buffer + used, email_key, sizeof(email_key) - 1);
    used += sizeof(email_key) - 1;
    buffer[used] = '\0';
    if (append_url_encoded(buffer, buffer_size, &used, email) < 0 ||
        used + sizeof(display_key) > buffer_size)
        return -1;
    memcpy(buffer + used, display_key, sizeof(display_key) - 1);
    used += sizeof(display_key) - 1;
    buffer[used] = '\0';
    return append_url_encoded(buffer, buffer_size, &used, display_name);
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
        fprintf(stderr, "invalid channel id\n");
        return 2;
    }
    int channel_id = (int)channel_value;

    /* Two independent TCP clients: one observer and one registration publisher. */
    MQClient observer;
    MQClient registrar;
    if (MQ_client_connect(&observer, argv[1], (uint16_t)port) < 0) {
        perror("observer connect");
        return 1;
    }
    if (MQ_client_subscribe(&observer, channel_id) < 0) {
        fprintf(stderr, "observer could not subscribe to channel %d\n", channel_id);
        MQ_client_disconnect(&observer);
        return 1;
    }
    if (MQ_client_connect(&registrar, argv[1], (uint16_t)port) < 0) {
        perror("registrar connect");
        MQ_client_disconnect(&observer);
        return 1;
    }
    if (MQ_client_start_monitor(&observer, 100, print_user_created, NULL) < 0) {
        fprintf(stderr, "could not start observer monitor\n");
        MQ_client_disconnect(&registrar);
        MQ_client_disconnect(&observer);
        return 1;
    }

    printf("Observer and registrar are connected. Channel %d is ready.\n",
           channel_id);
    char username[128];
    char email[192];
    char display_name[128];
    if (read_form_field("Username", username, sizeof(username)) < 0 ||
        read_form_field("Email", email, sizeof(email)) < 0 ||
        read_form_field("Display name", display_name, sizeof(display_name)) < 0) {
        MQ_client_disconnect(&registrar);
        MQ_client_disconnect(&observer);
        return 1;
    }

    char flattened[1024];
    if (flatten_user(username, email, display_name, flattened,
                     sizeof(flattened)) < 0) {
        fprintf(stderr, "could not flatten user data into the message buffer\n");
        MQ_client_disconnect(&registrar);
        MQ_client_disconnect(&observer);
        return 1;
    }
    if (MQ_client_publish(&registrar, channel_id, "USER_CREATED", flattened,
                          strlen(flattened)) < 0) {
        fprintf(stderr, "could not publish user-created event\n");
        MQ_client_disconnect(&registrar);
        MQ_client_disconnect(&observer);
        return 1;
    }
    printf("Registration event published. Waiting for observer notification...\n");
    for (int i = 0; i < 200 && !atomic_load(&notification_received); ++i) {
        struct timespec delay = {0, 10000000L};
        nanosleep(&delay, NULL);
    }
    if (!atomic_load(&notification_received)) {
        fprintf(stderr, "observer did not receive the event before timeout\n");
        MQ_client_disconnect(&registrar);
        MQ_client_disconnect(&observer);
        return 1;
    }
    MQ_client_disconnect(&registrar);
    MQ_client_disconnect(&observer);
    return 0;
}
