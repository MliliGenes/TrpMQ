CC ?= cc
CFLAGS ?= -Wall -Wextra -Wpedantic -std=c11 -O2
CPPFLAGS ?= -D_POSIX_C_SOURCE=200809L
LDLIBS ?= -pthread

COMMON = broker.o channel.o

all: mq_server mq_client mq_user_demo

mq_server: server_main.o $(COMMON)
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

mq_client: client_main.o client.o broker.o channel.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

mq_user_demo: user_demo.o client.o broker.o channel.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

%.o: %.c TrpMQ.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o mq_server mq_client mq_user_demo

.PHONY: all clean
