#include "TrpMQ.h"

static int read_field(const char *label, char *buffer, size_t capacity)
{
    printf("%s: ", label);
    fflush(stdout);
    if (!fgets(buffer, (int)capacity, stdin))
        return -1;
    size_t length = strlen(buffer);
    if (length && buffer[length - 1] == '\n') {
        buffer[--length] = '\0';
    } else if (!feof(stdin)) {
        int ch;
        while ((ch = getchar()) != '\n' && ch != EOF) {}
        fprintf(stderr, "%s is too long (maximum %zu characters).\n",
                label, capacity - 1);
        return -1;
    }
    if (!length) {
        fprintf(stderr, "%s cannot be empty.\n", label);
        return -1;
    }
    return 0;
}

static int append_encoded(char *out, size_t cap, size_t *used, const char *value)
{
    static const char hex[] = "0123456789ABCDEF";
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p) {
        unsigned char c = *p;
        int plain = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                    c == '.' || c == '~';
        size_t needed = plain ? 1 : 3;
        if (*used + needed >= cap)
            return -1;
        if (plain) {
            out[(*used)++] = (char)c;
        } else {
            out[(*used)++] = '%';
            out[(*used)++] = hex[c >> 4];
            out[(*used)++] = hex[c & 0x0f];
        }
    }
    out[*used] = '\0';
    return 0;
}

static int flatten_user(const char *username, const char *email,
                        const char *display_name, char *out, size_t cap)
{
    size_t used = 0;
    const char *prefix = "event=user_created&username=";
    const char *email_key = "&email=";
    const char *display_key = "&display_name=";
    size_t prefix_len = strlen(prefix);
    if (prefix_len >= cap)
        return -1;
    memcpy(out, prefix, prefix_len + 1);
    used = prefix_len;
    if (append_encoded(out, cap, &used, username) < 0 ||
        used + strlen(email_key) >= cap)
        return -1;
    memcpy(out + used, email_key, strlen(email_key) + 1);
    used += strlen(email_key);
    if (append_encoded(out, cap, &used, email) < 0 ||
        used + strlen(display_key) >= cap)
        return -1;
    memcpy(out + used, display_key, strlen(display_key) + 1);
    used += strlen(display_key);
    return append_encoded(out, cap, &used, display_name);
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

    MQClient client;
    if (MQ_client_connect(&client, argv[1], (uint16_t)port) < 0) {
        perror("connect");
        return 1;
    }
    char username[128];
    char email[192];
    char display_name[128];
    if (read_field("Username", username, sizeof(username)) < 0 ||
        read_field("Email", email, sizeof(email)) < 0 ||
        read_field("Display name", display_name, sizeof(display_name)) < 0) {
        MQ_client_disconnect(&client);
        return 1;
    }
    char flattened[1024];
    if (flatten_user(username, email, display_name, flattened,
                     sizeof(flattened)) < 0) {
        fprintf(stderr, "user data did not fit in the message buffer\n");
        MQ_client_disconnect(&client);
        return 1;
    }
    if (MQ_client_publish(&client, (int)channel_value, "USER_CREATED",
                          flattened, strlen(flattened)) < 0) {
        fprintf(stderr, "publish failed\n");
        MQ_client_disconnect(&client);
        return 1;
    }
    printf("Published USER_CREATED on channel %ld: %s\n",
           channel_value, flattened);
    MQ_client_disconnect(&client);
    return 0;
}
