#include "kv_store.h"
#include <ctype.h>

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

// Client function to put a key-value pair
bool kv_client_put(int sockfd, const char* key, const char* value) {
    if (sockfd < 0 || !key || !value) {
        return false;
    }
    
    // Create message
    Message msg;
    msg.op_code = OP_PUT;
    strncpy(msg.key, key, MAX_KEY_SIZE - 1);
    msg.key[MAX_KEY_SIZE - 1] = '\0';
    strncpy(msg.value, value, MAX_VALUE_SIZE - 1);
    msg.value[MAX_VALUE_SIZE - 1] = '\0';
    
    // Send message
    if (send(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Receive response
    if (recv(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Check if we need to redirect
    if (msg.status == -1) {
        // We would need to get the new node's IP and port from node list
        // For simplicity, this is not implemented here
        return false;
    }
    
    return msg.status == 1;
}

// Client function to get a value by key
bool kv_client_get(int sockfd, const char* key, char* value) {
    if (sockfd < 0 || !key || !value) {
        return false;
    }
    
    // Create message
    Message msg;
    msg.op_code = OP_GET;
    strncpy(msg.key, key, MAX_KEY_SIZE - 1);
    msg.key[MAX_KEY_SIZE - 1] = '\0';
    
    // Send message
    if (send(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Receive response
    if (recv(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Check if we need to redirect
    if (msg.status == -1) {
        // We would need to get the new node's IP and port from node list
        // For simplicity, this is not implemented here
        return false;
    }
    
    if (msg.status == 1) {
        strncpy(value, msg.value, MAX_VALUE_SIZE);
        return true;
    }
    
    return false;
}

// Client function to delete a key-value pair
bool kv_client_delete(int sockfd, const char* key) {
    if (sockfd < 0 || !key) {
        return false;
    }
    
    // Create message
    Message msg;
    msg.op_code = OP_DELETE;
    strncpy(msg.key, key, MAX_KEY_SIZE - 1);
    msg.key[MAX_KEY_SIZE - 1] = '\0';
    
    // Send message
    if (send(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Receive response
    if (recv(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Check if we need to redirect
    if (msg.status == -1) {
        // We would need to get the new node's IP and port from node list
        // For simplicity, this is not implemented here
        return false;
    }
    
    return msg.status == 1;
}

// Client function to list all keys
bool kv_client_list_keys(int sockfd, char* buffer, int buffer_size) {
    if (sockfd < 0 || !buffer || buffer_size <= 0) {
        return false;
    }
    
    // Create message
    Message msg;
    msg.op_code = OP_LIST_KEYS;
    
    // Send message
    if (send(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Receive response
    if (recv(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    if (msg.status == 1) {
        strncpy(buffer, msg.value, buffer_size - 1);
        buffer[buffer_size - 1] = '\0';
        return true;
    }
    
    return false;
}

// Client function to join the cluster
bool kv_client_join(int sockfd, const char* ip, int port) {
    if (sockfd < 0 || !ip) {
        return false;
    }
    
    // Create message
    Message msg;
    msg.op_code = OP_NODE_JOIN;
    strncpy(msg.key, ip, MAX_KEY_SIZE - 1);
    msg.key[MAX_KEY_SIZE - 1] = '\0';
    snprintf(msg.value, MAX_VALUE_SIZE, "%d", port);
    
    // Send message
    if (send(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Receive response
    if (recv(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    return msg.status == 1;
}

// Client function to leave the cluster
bool kv_client_leave(int sockfd, const char* ip, int port) {
    if (sockfd < 0 || !ip) {
        return false;
    }
    
    // Create message
    Message msg;
    msg.op_code = OP_NODE_LEAVE;
    strncpy(msg.key, ip, MAX_KEY_SIZE - 1);
    msg.key[MAX_KEY_SIZE - 1] = '\0';
    snprintf(msg.value, MAX_VALUE_SIZE, "%d", port);
    
    // Send message
    if (send(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    // Receive response
    if (recv(sockfd, &msg, sizeof(Message), 0) <= 0) {
        return false;
    }
    
    return msg.status == 1;
}

// Sample client program
int main(int argc, char* argv[]) {
    const char* server_ip = "127.0.0.1";
    int server_port = DEFAULT_PORT;
    
    // Parse command line arguments
    if (argc > 1) {
        server_ip = argv[1];
    }
    
    if (argc > 2) {
        server_port = atoi(argv[2]);
    }
    
    // Connect to server
    int sockfd = connect_to_server(server_ip, server_port);
    if (sockfd < 0) {
        fprintf(stderr, "Failed to connect to server at %s:%d\n", server_ip, server_port);
        return 1;
    }
    
    printf("Connected to server at %s:%d\n", server_ip, server_port);
    
    // Interactive command loop
    char command[20];
    char key[MAX_KEY_SIZE];
    char value[MAX_VALUE_SIZE];
    char buffer[MAX_VALUE_SIZE];
    
    while (1) {
        printf("\nCommands: PUT, GET, DELETE, LIST, JOIN, LEAVE, QUIT\n");
        printf("> ");
        
        if (scanf("%19s", command) != 1) {
            break;
        }
        
        // Convert command to uppercase
        for (int i = 0; command[i]; i++) {
            command[i] = toupper(command[i]);
        }
        
        if (strcmp(command, "PUT") == 0) {
            // Get key and value
            printf("Key: ");
            if (scanf("%127s", key) != 1) {
                continue;
            }
            
            printf("Value: ");
            if (scanf(" %1023[^\n]", value) != 1) {
                continue;
            }
            
            // Put key-value pair
            if (kv_client_put(sockfd, key, value)) {
                printf("Successfully stored key '%s'\n", key);
            } else {
                printf("Failed to store key '%s'\n", key);
            }
        } 
        else if (strcmp(command, "GET") == 0) {
            // Get key
            printf("Key: ");
            if (scanf("%127s", key) != 1) {
                continue;
            }
            
            // Get value
            if (kv_client_get(sockfd, key, value)) {
                printf("Value: %s\n", value);
            } else {
                printf("Key '%s' not found\n", key);
            }
        } 
        else if (strcmp(command, "DELETE") == 0) {
            // Get key
            printf("Key: ");
            if (scanf("%127s", key) != 1) {
                continue;
            }
            
            // Delete key
            if (kv_client_delete(sockfd, key)) {
                printf("Successfully deleted key '%s'\n", key);
            } else {
                printf("Failed to delete key '%s'\n", key);
            }
        } 
        else if (strcmp(command, "LIST") == 0) {
            // List keys
            if (kv_client_list_keys(sockfd, buffer, MAX_VALUE_SIZE)) {
                printf("Keys:\n%s", buffer);
            } else {
                printf("Failed to list keys\n");
            }
        } 
        else if (strcmp(command, "JOIN") == 0) {
            // Get IP and port
            printf("IP: ");
            if (scanf("%15s", key) != 1) {
                continue;
            }
            
            printf("Port: ");
            int port;
            if (scanf("%d", &port) != 1) {
                continue;
            }
            
            // Join cluster
            if (kv_client_join(sockfd, key, port)) {
                printf("Successfully joined cluster\n");
            } else {
                printf("Failed to join cluster\n");
            }
        } 
        else if (strcmp(command, "LEAVE") == 0) {
            // Get IP and port
            printf("IP: ");
            if (scanf("%15s", key) != 1) {
                continue;
            }
            
            printf("Port: ");
            int port;
            if (scanf("%d", &port) != 1) {
                continue;
            }
            
            // Leave cluster
            if (kv_client_leave(sockfd, key, port)) {
                printf("Successfully left cluster\n");
            } else {
                printf("Failed to leave cluster\n");
            }
        } 
        else if (strcmp(command, "QUIT") == 0) {
            break;
        } 
        else {
            printf("Unknown command: %s\n", command);
        }
    }
    
    // Close connection
    close(sockfd);
    
    return 0;
} 