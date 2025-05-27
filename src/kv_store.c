#include "kv_store.h"

// Simple hash function for distributing keys
unsigned int hash_key(const char* key) {
    unsigned int hash = 0;
    while (*key) {
        hash = (hash * 31) + (*key++);
    }
    return hash;
}

// Create directory if it doesn't exist
bool ensure_directory_exists(const char* path) {
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        // Directory doesn't exist, create it
        #ifdef _WIN32
        if (mkdir(path) != 0) {
        #else
        if (mkdir(path, 0755) != 0) {
        #endif
            fprintf(stderr, "Error creating directory %s: %s\n", path, strerror(errno));
            return false;
        }
    }
    return true;
}

// Initialize key-value store
KVStore* kv_store_init(int capacity) {
    KVStore* store = (KVStore*)malloc(sizeof(KVStore));
    if (!store) {
        return NULL;
    }
    
    store->data = (KeyValuePair*)malloc(sizeof(KeyValuePair) * capacity);
    if (!store->data) {
        free(store);
        return NULL;
    }
    
    // Initialize all entries as invalid
    for (int i = 0; i < capacity; i++) {
        store->data[i].valid = false;
    }
    
    store->capacity = capacity;
    store->size = 0;
    pthread_mutex_init(&store->lock, NULL);
    
    // Initialize persistence-related fields
    store->persistence_enabled = false;
    store->op_count = 0;
    store->log_file = NULL;
    strncpy(store->data_dir, DATA_DIR, sizeof(store->data_dir) - 1);
    store->data_dir[sizeof(store->data_dir) - 1] = '\0';
    
    return store;
}

// Enable persistence for the key-value store
bool kv_store_enable_persistence(KVStore* store, const char* data_dir) {
    if (!store || !data_dir) {
        return false;
    }
    
    pthread_mutex_lock(&store->lock);
    
    // Set the data directory
    strncpy(store->data_dir, data_dir, sizeof(store->data_dir) - 1);
    store->data_dir[sizeof(store->data_dir) - 1] = '\0';
    
    // Ensure the data directory exists
    if (!ensure_directory_exists(store->data_dir)) {
        pthread_mutex_unlock(&store->lock);
        return false;
    }
    
    // Create a log file path
    char log_path[512];
    snprintf(log_path, sizeof(log_path), "%s/operations.log", store->data_dir);
    
    // Open the log file for append
    store->log_file = fopen(log_path, "a+");
    if (!store->log_file) {
        fprintf(stderr, "Error opening log file: %s\n", strerror(errno));
        pthread_mutex_unlock(&store->lock);
        return false;
    }
    
    // Set persistence as enabled
    store->persistence_enabled = true;
    
    // Recover data from existing logs if they exist
    if (!kv_store_recover_from_logs(store)) {
        fprintf(stderr, "Warning: Failed to recover data from logs\n");
        // We continue anyway since we might be starting fresh
    }
    
    pthread_mutex_unlock(&store->lock);
    return true;
}

// Log an operation to the append-only log
bool kv_store_log_operation(KVStore* store, OperationCode op, const char* key, const char* value) {
    if (!store || !store->persistence_enabled || !store->log_file) {
        return false;
    }
    
    LogEntry entry;
    entry.op_code = op;
    entry.timestamp = time(NULL);
    strncpy(entry.key, key, MAX_KEY_SIZE - 1);
    entry.key[MAX_KEY_SIZE - 1] = '\0';
    
    if (value) {
        strncpy(entry.value, value, MAX_VALUE_SIZE - 1);
        entry.value[MAX_VALUE_SIZE - 1] = '\0';
    } else {
        entry.value[0] = '\0';
    }
    
    // Write the entry to the log file
    size_t items_written = fwrite(&entry, sizeof(LogEntry), 1, store->log_file);
    fflush(store->log_file); // Ensure it's written to disk
    
    // Check if we need to create a snapshot
    store->op_count++;
    if (store->op_count >= SNAPSHOT_THRESHOLD) {
        kv_store_create_snapshot(store);
        store->op_count = 0;
    }
    
    return items_written == 1;
}

// Create a snapshot of the current state
bool kv_store_create_snapshot(KVStore* store) {
    if (!store || !store->persistence_enabled) {
        return false;
    }
    
    pthread_mutex_lock(&store->lock);
    
    // Create a snapshot file path with timestamp
    char snapshot_path[512];
    time_t now = time(NULL);
    snprintf(snapshot_path, sizeof(snapshot_path), "%s/snapshot_%ld.dat", 
             store->data_dir, (long)now);
    
    // Open the snapshot file
    FILE* snapshot_file = fopen(snapshot_path, "wb");
    if (!snapshot_file) {
        fprintf(stderr, "Error creating snapshot file: %s\n", strerror(errno));
        pthread_mutex_unlock(&store->lock);
        return false;
    }
    
    // Write the number of valid entries
    fwrite(&store->size, sizeof(int), 1, snapshot_file);
    
    // Write all valid key-value pairs
    for (int i = 0; i < store->capacity; i++) {
        if (store->data[i].valid) {
            fwrite(&store->data[i], sizeof(KeyValuePair), 1, snapshot_file);
        }
    }
    
    fclose(snapshot_file);
    
    // After creating a snapshot, start a new log file
    if (store->log_file) {
        fclose(store->log_file);
    }
    
    // Create a new log file path
    char log_path[512];
    snprintf(log_path, sizeof(log_path), "%s/operations_%ld.log", store->data_dir, (long)now);
    
    // Open the new log file
    store->log_file = fopen(log_path, "a+");
    if (!store->log_file) {
        fprintf(stderr, "Error opening new log file: %s\n", strerror(errno));
        // We continue with persistence disabled
        store->persistence_enabled = false;
        pthread_mutex_unlock(&store->lock);
        return false;
    }
    
    pthread_mutex_unlock(&store->lock);
    return true;
}

// Find the most recent snapshot file
static char* find_latest_snapshot(const char* data_dir) {
    DIR* dir = opendir(data_dir);
    if (!dir) {
        return NULL;
    }
    
    struct dirent* entry;
    time_t latest_time = 0;
    char* latest_snapshot = NULL;
    
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "snapshot_", 9) == 0 && 
            strstr(entry->d_name, ".dat") != NULL) {
            
            // Extract timestamp from filename
            time_t snapshot_time = atol(entry->d_name + 9);
            
            if (snapshot_time > latest_time) {
                latest_time = snapshot_time;
                free(latest_snapshot);
                latest_snapshot = strdup(entry->d_name);
            }
        }
    }
    
    closedir(dir);
    return latest_snapshot;
}

// Find all log files newer than a given timestamp
static char** find_newer_logs(const char* data_dir, time_t after_time, int* count) {
    DIR* dir = opendir(data_dir);
    if (!dir) {
        *count = 0;
        return NULL;
    }
    
    // First, count matching files
    struct dirent* entry;
    *count = 0;
    
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "operations_", 11) == 0 && 
            strstr(entry->d_name, ".log") != NULL) {
            
            // Extract timestamp from filename
            time_t log_time = atol(entry->d_name + 11);
            
            if (log_time >= after_time) {
                (*count)++;
            }
        }
    }
    
    rewinddir(dir);
    
    if (*count == 0) {
        closedir(dir);
        return NULL;
    }
    
    // Allocate array for filenames
    char** log_files = (char**)malloc(sizeof(char*) * (*count));
    if (!log_files) {
        closedir(dir);
        *count = 0;
        return NULL;
    }
    
    // Fill the array
    int index = 0;
    while ((entry = readdir(dir)) != NULL && index < *count) {
        if (strncmp(entry->d_name, "operations_", 11) == 0 && 
            strstr(entry->d_name, ".log") != NULL) {
            
            // Extract timestamp from filename
            time_t log_time = atol(entry->d_name + 11);
            
            if (log_time >= after_time) {
                log_files[index++] = strdup(entry->d_name);
            }
        }
    }
    
    closedir(dir);
    return log_files;
}

// Recover data from logs and snapshots
bool kv_store_recover_from_logs(KVStore* store) {
    if (!store || !store->persistence_enabled) {
        return false;
    }
    
    // Find the latest snapshot
    char* latest_snapshot = find_latest_snapshot(store->data_dir);
    time_t snapshot_time = 0;
    
    // Load data from snapshot if available
    if (latest_snapshot) {
        char snapshot_path[512];
        snprintf(snapshot_path, sizeof(snapshot_path), "%s/%s", store->data_dir, latest_snapshot);
        
        FILE* snapshot_file = fopen(snapshot_path, "rb");
        if (snapshot_file) {
            // Extract timestamp from filename
            snapshot_time = atol(latest_snapshot + 9);
            
            // Read the number of entries
            int num_entries;
            if (fread(&num_entries, sizeof(int), 1, snapshot_file) == 1) {
                // Read each key-value pair
                KeyValuePair pair;
                for (int i = 0; i < num_entries; i++) {
                    if (fread(&pair, sizeof(KeyValuePair), 1, snapshot_file) == 1) {
                        // Find an empty slot and insert
                        for (int j = 0; j < store->capacity; j++) {
                            if (!store->data[j].valid) {
                                memcpy(&store->data[j], &pair, sizeof(KeyValuePair));
                                store->size++;
                                break;
                            }
                        }
                    }
                }
            }
            
            fclose(snapshot_file);
        }
        
        free(latest_snapshot);
    }
    
    // Find log files newer than the snapshot
    int log_count = 0;
    char** log_files = find_newer_logs(store->data_dir, snapshot_time, &log_count);
    
    // Process each log file
    for (int i = 0; i < log_count; i++) {
        char log_path[512];
        snprintf(log_path, sizeof(log_path), "%s/%s", store->data_dir, log_files[i]);
        
        FILE* log_file = fopen(log_path, "rb");
        if (log_file) {
            LogEntry entry;
            
            // Read and apply each operation
            while (fread(&entry, sizeof(LogEntry), 1, log_file) == 1) {
                switch (entry.op_code) {
                    case OP_PUT:
                        kv_store_put(store, entry.key, entry.value);
                        break;
                        
                    case OP_DELETE:
                        kv_store_delete(store, entry.key);
                        break;
                        
                    default:
                        // Ignore other operations
                        break;
                }
            }
            
            fclose(log_file);
        }
        
        free(log_files[i]);
    }
    
    free(log_files);
    
    return true;
}

// Clean up resources
void kv_store_destroy(KVStore* store) {
    if (store) {
        // Create a final snapshot if persistence is enabled
        if (store->persistence_enabled) {
            kv_store_create_snapshot(store);
            
            if (store->log_file) {
                fclose(store->log_file);
            }
        }
        
        pthread_mutex_destroy(&store->lock);
        if (store->data) {
            free(store->data);
        }
        free(store);
    }
}

// Add or update a key-value pair
bool kv_store_put(KVStore* store, const char* key, const char* value) {
    if (!store || !key || !value) {
        return false;
    }
    
    pthread_mutex_lock(&store->lock);
    
    // Check if key already exists
    for (int i = 0; i < store->capacity; i++) {
        if (store->data[i].valid && strcmp(store->data[i].key, key) == 0) {
            // Update existing key
            strncpy(store->data[i].value, value, MAX_VALUE_SIZE - 1);
            store->data[i].value[MAX_VALUE_SIZE - 1] = '\0';
            
            // Log the operation if persistence is enabled
            if (store->persistence_enabled) {
                kv_store_log_operation(store, OP_PUT, key, value);
            }
            
            pthread_mutex_unlock(&store->lock);
            return true;
        }
    }
    
    // Key doesn't exist, find an empty slot
    if (store->size >= store->capacity) {
        // Store is full
        pthread_mutex_unlock(&store->lock);
        return false;
    }
    
    for (int i = 0; i < store->capacity; i++) {
        if (!store->data[i].valid) {
            // Found empty slot
            strncpy(store->data[i].key, key, MAX_KEY_SIZE - 1);
            strncpy(store->data[i].value, value, MAX_VALUE_SIZE - 1);
            store->data[i].key[MAX_KEY_SIZE - 1] = '\0';
            store->data[i].value[MAX_VALUE_SIZE - 1] = '\0';
            store->data[i].valid = true;
            store->size++;
            
            // Log the operation if persistence is enabled
            if (store->persistence_enabled) {
                kv_store_log_operation(store, OP_PUT, key, value);
            }
            
            pthread_mutex_unlock(&store->lock);
            return true;
        }
    }
    
    pthread_mutex_unlock(&store->lock);
    return false;
}

// Retrieve a value by key
bool kv_store_get(KVStore* store, const char* key, char* value) {
    if (!store || !key || !value) {
        return false;
    }
    
    pthread_mutex_lock(&store->lock);
    
    for (int i = 0; i < store->capacity; i++) {
        if (store->data[i].valid && strcmp(store->data[i].key, key) == 0) {
            strncpy(value, store->data[i].value, MAX_VALUE_SIZE);
            pthread_mutex_unlock(&store->lock);
            return true;
        }
    }
    
    pthread_mutex_unlock(&store->lock);
    return false;
}

// Delete a key-value pair
bool kv_store_delete(KVStore* store, const char* key) {
    if (!store || !key) {
        return false;
    }
    
    pthread_mutex_lock(&store->lock);
    
    for (int i = 0; i < store->capacity; i++) {
        if (store->data[i].valid && strcmp(store->data[i].key, key) == 0) {
            store->data[i].valid = false;
            store->size--;
            
            // Log the operation if persistence is enabled
            if (store->persistence_enabled) {
                kv_store_log_operation(store, OP_DELETE, key, NULL);
            }
            
            pthread_mutex_unlock(&store->lock);
            return true;
        }
    }
    
    pthread_mutex_unlock(&store->lock);
    return false;
}

// List all keys in the store
void kv_store_list_keys(KVStore* store, char* buffer, int buffer_size) {
    if (!store || !buffer || buffer_size <= 0) {
        if (buffer && buffer_size > 0) {
            buffer[0] = '\0';
        }
        return;
    }
    
    pthread_mutex_lock(&store->lock);
    
    buffer[0] = '\0';
    int pos = 0;
    
    for (int i = 0; i < store->capacity && pos < buffer_size - 1; i++) {
        if (store->data[i].valid) {
            int remaining = buffer_size - pos - 1;
            int key_len = strlen(store->data[i].key);
            
            if (remaining >= key_len + 1) { // +1 for newline or null terminator
                pos += snprintf(buffer + pos, remaining + 1, "%s\n", store->data[i].key);
            } else {
                break;
            }
        }
    }
    
    pthread_mutex_unlock(&store->lock);
}

// Initialize node list
NodeList* node_list_init() {
    NodeList* list = (NodeList*)malloc(sizeof(NodeList));
    if (!list) {
        return NULL;
    }
    
    list->count = 0;
    list->current_node_idx = -1;
    pthread_mutex_init(&list->lock, NULL);
    
    return list;
}

// Clean up node list
void node_list_destroy(NodeList* list) {
    if (list) {
        pthread_mutex_destroy(&list->lock);
        free(list);
    }
}

// Add a node to the list
bool node_list_add(NodeList* list, const char* ip, int port) {
    if (!list || !ip || list->count >= MAX_NODES) {
        return false;
    }
    
    pthread_mutex_lock(&list->lock);
    
    // Check if node already exists
    for (int i = 0; i < list->count; i++) {
        if (strcmp(list->nodes[i].ip, ip) == 0 && list->nodes[i].port == port) {
            list->nodes[i].active = true;
            pthread_mutex_unlock(&list->lock);
            return true;
        }
    }
    
    // Add new node
    strncpy(list->nodes[list->count].ip, ip, 15);
    list->nodes[list->count].ip[15] = '\0';
    list->nodes[list->count].port = port;
    list->nodes[list->count].active = true;
    list->count++;
    
    // If this is the first node, set it as current
    if (list->count == 1) {
        list->current_node_idx = 0;
    }
    
    pthread_mutex_unlock(&list->lock);
    return true;
}

// Remove a node from the list
bool node_list_remove(NodeList* list, const char* ip, int port) {
    if (!list || !ip) {
        return false;
    }
    
    pthread_mutex_lock(&list->lock);
    
    for (int i = 0; i < list->count; i++) {
        if (strcmp(list->nodes[i].ip, ip) == 0 && list->nodes[i].port == port) {
            // Mark as inactive
            list->nodes[i].active = false;
            
            // If this was the current node, select a new one
            if (list->current_node_idx == i) {
                int new_idx = -1;
                for (int j = 0; j < list->count; j++) {
                    if (list->nodes[j].active) {
                        new_idx = j;
                        break;
                    }
                }
                list->current_node_idx = new_idx;
            }
            
            pthread_mutex_unlock(&list->lock);
            return true;
        }
    }
    
    pthread_mutex_unlock(&list->lock);
    return false;
}

// Determine which node should handle a key
int node_for_key(NodeList* list, const char* key) {
    if (!list || !key || list->count == 0) {
        return -1;
    }
    
    pthread_mutex_lock(&list->lock);
    
    // Count active nodes
    int active_count = 0;
    for (int i = 0; i < list->count; i++) {
        if (list->nodes[i].active) {
            active_count++;
        }
    }
    
    if (active_count == 0) {
        pthread_mutex_unlock(&list->lock);
        return -1;
    }
    
    // Use consistent hashing to determine node
    unsigned int hash = hash_key(key);
    int node_idx = hash % active_count;
    
    // Map to an actual active node
    int curr_active = 0;
    for (int i = 0; i < list->count; i++) {
        if (list->nodes[i].active) {
            if (curr_active == node_idx) {
                pthread_mutex_unlock(&list->lock);
                return i;
            }
            curr_active++;
        }
    }
    
    pthread_mutex_unlock(&list->lock);
    return -1;
}

// Redistribute data when node configuration changes
void distribute_data(KVStore* store, NodeList* list) {
    if (!store || !list) {
        return;
    }
    
    // This is a simplified implementation
    // In a real system, you would:
    // 1. Collect all key-value pairs
    // 2. Determine which node should own each pair
    // 3. Send data to the appropriate nodes
    
    printf("Data redistribution would happen here in a real implementation\n");
} 