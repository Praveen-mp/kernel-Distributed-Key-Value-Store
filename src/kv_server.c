#include "kv_store.h"
#include <ctype.h>  // For isdigit function

// Thread data structure
typedef struct {
    int client_fd;
    KVStore* store;
    NodeList* nodes;
} ThreadData;

// Thread function to handle client connections
void* client_thread(void* arg) {
    ThreadData* data = (ThreadData*)arg;
    
    handle_client(data->client_fd, data->store, data->nodes);
    
    close(data->client_fd);
    free(data);
    
    return NULL;
}

// Function to handle a client connection
void handle_client(int client_fd, KVStore* store, NodeList* list) {
    Message msg;
    char buffer[MAX_VALUE_SIZE];
    
    // Read message from client
    ssize_t bytes_read = recv(client_fd, &msg, sizeof(Message), 0);
    if (bytes_read <= 0) {
        return;
    }
    
    // Process message based on operation code
    switch (msg.op_code) {
        case OP_GET: {
            // Check if this node should handle the key
            int node_idx = node_for_key(list, msg.key);
            if (node_idx != list->current_node_idx && node_idx >= 0) {
                // Forward to correct node
                msg.status = -1; // Indicate redirection
                send(client_fd, &msg, sizeof(Message), 0);
                break;
            }
            
            if (kv_store_get(store, msg.key, msg.value)) {
                msg.status = 1; // Success
            } else {
                msg.status = 0; // Key not found
            }
            send(client_fd, &msg, sizeof(Message), 0);
            break;
        }
            
        case OP_PUT: {
            // Check if this node should handle the key
            int node_idx = node_for_key(list, msg.key);
            if (node_idx != list->current_node_idx && node_idx >= 0) {
                // Forward to correct node
                msg.status = -1; // Indicate redirection
                send(client_fd, &msg, sizeof(Message), 0);
                break;
            }
            
            if (kv_store_put(store, msg.key, msg.value)) {
                msg.status = 1; // Success
                
                // Replicate to other nodes
                replicate_to_nodes(list, &msg);
            } else {
                msg.status = 0; // Failure
            }
            send(client_fd, &msg, sizeof(Message), 0);
            break;
        }
            
        case OP_DELETE: {
            // Check if this node should handle the key
            int node_idx = node_for_key(list, msg.key);
            if (node_idx != list->current_node_idx && node_idx >= 0) {
                // Forward to correct node
                msg.status = -1; // Indicate redirection
                send(client_fd, &msg, sizeof(Message), 0);
                break;
            }
            
            if (kv_store_delete(store, msg.key)) {
                msg.status = 1; // Success
                
                // Replicate to other nodes
                replicate_to_nodes(list, &msg);
            } else {
                msg.status = 0; // Key not found
            }
            send(client_fd, &msg, sizeof(Message), 0);
            break;
        }
            
        case OP_REPLICATE: {
            // This is a replication message from another node
            if (msg.op_code == OP_PUT) {
                kv_store_put(store, msg.key, msg.value);
            } else if (msg.op_code == OP_DELETE) {
                kv_store_delete(store, msg.key);
            }
            msg.status = 1;
            send(client_fd, &msg, sizeof(Message), 0);
            break;
        }
            
        case OP_NODE_JOIN: {
            // Add the new node to the list
            node_list_add(list, msg.key, atoi(msg.value));
            msg.status = 1;
            send(client_fd, &msg, sizeof(Message), 0);
            
            // Redistribute data
            distribute_data(store, list);
            break;
        }
            
        case OP_NODE_LEAVE: {
            // Remove the node from the list
            node_list_remove(list, msg.key, atoi(msg.value));
            msg.status = 1;
            send(client_fd, &msg, sizeof(Message), 0);
            
            // Redistribute data
            distribute_data(store, list);
            break;
        }
            
        case OP_LIST_KEYS: {
            // Get list of keys
            kv_store_list_keys(store, buffer, MAX_VALUE_SIZE);
            strncpy(msg.value, buffer, MAX_VALUE_SIZE - 1);
            msg.value[MAX_VALUE_SIZE - 1] = '\0';
            msg.status = 1;
            send(client_fd, &msg, sizeof(Message), 0);
            break;
        }
            
        default:
            // Unknown operation
            msg.status = -2;
            send(client_fd, &msg, sizeof(Message), 0);
            break;
    }
}

// Replicate operation to other nodes
void replicate_to_nodes(NodeList* list, Message* msg) {
    if (!list || !msg) {
        return;
    }
    
    pthread_mutex_lock(&list->lock);
    
    Message repl_msg;
    repl_msg.op_code = OP_REPLICATE;
    strncpy(repl_msg.key, msg->key, MAX_KEY_SIZE);
    strncpy(repl_msg.value, msg->value, MAX_VALUE_SIZE);
    
    // Send to all active nodes except current
    for (int i = 0; i < list->count; i++) {
        if (i != list->current_node_idx && list->nodes[i].active) {
            int sockfd = connect_to_server(list->nodes[i].ip, list->nodes[i].port);
            if (sockfd >= 0) {
                send(sockfd, &repl_msg, sizeof(Message), 0);
                
                // Receive acknowledgment
                recv(sockfd, &repl_msg, sizeof(Message), 0);
                
                close(sockfd);
            } else {
                // Connection failed, mark node as inactive
                list->nodes[i].active = false;
            }
        }
    }
    
    pthread_mutex_unlock(&list->lock);
}

// Start the server
int start_server(KVStore* store, NodeList* list, int port) {
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    
    // Create socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        return -1;
    }
    
    // Set socket options
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt");
        close(server_fd);
        return -1;
    }
    
    // Configure address
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);
    
    // Bind socket to address
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        close(server_fd);
        return -1;
    }
    
    // Start listening for connections
    if (listen(server_fd, 10) < 0) {
        perror("listen");
        close(server_fd);
        return -1;
    }
    
    printf("Server started on port %d\n", port);
    
    // Get local IP address
    char hostname[128];
    gethostname(hostname, sizeof(hostname));
    struct hostent *h = gethostbyname(hostname);
    char *ip = inet_ntoa(*(struct in_addr *)h->h_addr_list[0]);
    
    // Add self to node list
    node_list_add(list, ip, port);
    list->current_node_idx = 0;
    
    // Accept connections
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd;
        
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }
        
        // Create thread data
        ThreadData* data = (ThreadData*)malloc(sizeof(ThreadData));
        if (!data) {
            close(client_fd);
            continue;
        }
        
        data->client_fd = client_fd;
        data->store = store;
        data->nodes = list;
        
        // Create thread to handle client
        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread, data) != 0) {
            perror("pthread_create");
            free(data);
            close(client_fd);
            continue;
        }
        
        // Detach thread
        pthread_detach(tid);
    }
    
    return 0;
}

// Connect to a server
int connect_to_server(const char* ip, int port) {
    int sockfd;
    struct sockaddr_in server_addr;
    
    // Create socket
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return -1;
    }
    
    // Configure server address
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    // Convert IP address
    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
        close(sockfd);
        return -1;
    }
    
    // Connect to server
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(sockfd);
        return -1;
    }
    
    return sockfd;
}

// Main function for the server
int main(int argc, char* argv[]) {
    int port = DEFAULT_PORT;
    const char* data_dir = DATA_DIR;
    bool enable_persistence = true;
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = atoi(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[i + 1];
            i++;
        } else if (strcmp(argv[i], "--no-persistence") == 0) {
            enable_persistence = false;
        } else if (isdigit(argv[i][0])) {
            // For backward compatibility: if first arg is a number, it's the port
            port = atoi(argv[i]);
        }
    }
    
    printf("Starting key-value store server on port %d\n", port);
    if (enable_persistence) {
        printf("Persistence enabled, data directory: %s\n", data_dir);
    } else {
        printf("Persistence disabled, data will be lost on shutdown\n");
    }
    
    // Initialize key-value store
    KVStore* store = kv_store_init(1000);
    if (!store) {
        fprintf(stderr, "Failed to initialize key-value store\n");
        return 1;
    }
    
    // Enable persistence if requested
    if (enable_persistence) {
        if (!kv_store_enable_persistence(store, data_dir)) {
            fprintf(stderr, "Warning: Failed to enable persistence, continuing without it\n");
        }
    }
    
    // Initialize node list
    NodeList* nodes = node_list_init();
    if (!nodes) {
        fprintf(stderr, "Failed to initialize node list\n");
        kv_store_destroy(store);
        return 1;
    }
    
    // Start server
    int result = start_server(store, nodes, port);
    
    // Clean up
    node_list_destroy(nodes);
    kv_store_destroy(store);
    
    return result == 0 ? 0 : 1;
} 