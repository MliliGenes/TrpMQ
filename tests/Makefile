ROOT := ..
CC ?= cc
CFLAGS ?= -Wall -Wextra -Wpedantic -std=c11 -O2
CPPFLAGS ?= -D_POSIX_C_SOURCE=200809L
LDLIBS ?= -pthread
COMMON := $(ROOT)/client.c $(ROOT)/broker.c $(ROOT)/channel.c

all: client1/client1 client2/client2

client1/client1: client1/main.c $(COMMON) $(ROOT)/TrpMQ.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -I$(ROOT) -o $@ $< $(COMMON) $(LDLIBS)

client2/client2: client2/main.c $(COMMON) $(ROOT)/TrpMQ.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -I$(ROOT) -o $@ $< $(COMMON) $(LDLIBS)

clean:
	rm -f client1/client1 client2/client2

.PHONY: all clean
