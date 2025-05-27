CC = gcc
CFLAGS = -Wall -Wextra -pthread
LDFLAGS = -pthread

all: kv_server kv_client

kv_server: src/kv_server.c src/kv_store.c src/kv_store.h
	$(CC) $(CFLAGS) -o kv_server src/kv_server.c src/kv_store.c $(LDFLAGS)

kv_client: src/kv_client.c src/kv_store.c src/kv_store.h
	$(CC) $(CFLAGS) -o kv_client src/kv_client.c src/kv_store.c $(LDFLAGS)

clean:
	rm -f kv_server kv_client

.PHONY: all clean