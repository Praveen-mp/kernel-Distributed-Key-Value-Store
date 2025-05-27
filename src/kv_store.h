#ifndef KV_STORE_H
#define KV_STORE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdbool.h>
#include <netdb.h>  // For gethostbyname
#include <fcntl.h>  // For file operations
#include <sys/stat.h> // For file stats
#include <errno.h>  // For error handling
#include <dirent.h> // For directory operations

#define MAX_KEY_SIZE 128
#define MAX_VALUE_SIZE 1024
#define MAX_NODES 10
#define DEFAULT_PORT 8080
#define DATA_DIR "./data"
#define SNAPSHOT_THRESHOLD 100 // Number of operations before creating a snapshot

// Operation codes
typedef enum {
    OP_GET = 1,
    OP_PUT = 2,
    OP_DELETE = 3,
    OP_REPLICATE = 4,
    OP_NODE_JOIN = 5,
    OP_NODE_LEAVE = 6,
    OP_LIST_KEYS = 7
} OperationCode;

// Data structures
typedef struct {
    char key[MAX_KEY_SIZE];
    char value[MAX_VALUE_SIZE];
    bool valid;
} KeyValuePair;

typedef struct {
    KeyValuePair* data;
    int capacity;
    int size;
    pthread_mutex_t lock;
    char data_dir[256];        // Directory for persistence
    int op_count;              // Count of operations since last snapshot
    FILE* log_file;            // File handle for the append-only log
    bool persistence_enabled;  // Flag to enable/disable persistence
} KVStore;

typedef struct {
    char ip[16];
    int port;
    bool active;
} Node;

typedef struct {
    Node nodes[MAX_NODES];
    int count;
    int current_node_idx;
    pthread_mutex_t lock;
} NodeList;

// Message format for network communication
typedef struct {
    OperationCode op_code;
    char key[MAX_KEY_SIZE];
    char value[MAX_VALUE_SIZE];
    int status;
} Message;

// Persistence-related log entry
typedef struct {
    OperationCode op_code;    // Operation type (PUT, DELETE)
    time_t timestamp;         // When the operation occurred
    char key[MAX_KEY_SIZE];   // Key affected
    char value[MAX_VALUE_SIZE]; // Value (for PUT operations)
} LogEntry;

// KVStore functions
KVStore* kv_store_init(int capacity);
void kv_store_destroy(KVStore* store);
bool kv_store_put(KVStore* store, const char* key, const char* value);
bool kv_store_get(KVStore* store, const char* key, char* value);
bool kv_store_delete(KVStore* store, const char* key);
void kv_store_list_keys(KVStore* store, char* buffer, int buffer_size);

// Persistence functions
bool kv_store_enable_persistence(KVStore* store, const char* data_dir);
bool kv_store_log_operation(KVStore* store, OperationCode op, const char* key, const char* value);
bool kv_store_create_snapshot(KVStore* store);
bool kv_store_recover_from_logs(KVStore* store);
bool ensure_directory_exists(const char* path);

// Node management functions
NodeList* node_list_init();
void node_list_destroy(NodeList* list);
bool node_list_add(NodeList* list, const char* ip, int port);
bool node_list_remove(NodeList* list, const char* ip, int port);
int node_for_key(NodeList* list, const char* key);
void distribute_data(KVStore* store, NodeList* list);

// Network functions for server
int start_server(KVStore* store, NodeList* list, int port);
void handle_client(int client_fd, KVStore* store, NodeList* list);
void replicate_to_nodes(NodeList* list, Message* msg);

// Network functions for client
int connect_to_server(const char* ip, int port);
bool kv_client_put(int sockfd, const char* key, const char* value);
bool kv_client_get(int sockfd, const char* key, char* value);
bool kv_client_delete(int sockfd, const char* key);
bool kv_client_list_keys(int sockfd, char* buffer, int buffer_size);

// Hashing function for consistent hashing
unsigned int hash_key(const char* key);

#endif // KV_STORE_H 