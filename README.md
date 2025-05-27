# Distributed Key-Value Store

A simple distributed key-value store implementation in C that provides basic functionality for storing, retrieving, and managing data across multiple nodes.

## Features

- In-memory key-value storage
- Data persistence with append-only logs and snapshots
- Distributed architecture with multiple nodes
- Key-based sharding using consistent hashing
- Replication of data across nodes for fault tolerance
- Interactive client for managing the key-value store
- Thread-safe operations

## Building the Project

To build the project, make sure you have GCC and the pthread library installed. Then run:

```
make
```

This will compile both the server and client components.

## Running the Server

To start a server node:

```
./kv_server [options]
```

### Server Options:

- `<port>`: Port number to listen on (default: 8080)
- `--port <port>`: Specify port number
- `--data-dir <directory>`: Specify data directory for persistence (default: ./data)
- `--no-persistence`: Disable data persistence

Examples:
```
./kv_server 3000                           # Run on port 3000 with default persistence
./kv_server --port 3000 --data-dir /tmp/kv # Run on port 3000 with persistence in /tmp/kv
./kv_server --no-persistence               # Run with persistence disabled
```

## Data Persistence

The server includes a persistence mechanism using:

1. **Append-only logs**: Each operation (PUT, DELETE) is recorded in a log file
2. **Periodic snapshots**: Complete store snapshots are taken after every 100 operations
3. **Automatic recovery**: Data is automatically recovered from logs and snapshots on startup

When the server starts, it:
1. Looks for the most recent snapshot
2. Loads the snapshot data (if available)
3. Applies all operations from logs created after the snapshot
4. Creates a new log file for future operations

This ensures that data is not lost even if the server crashes or is shut down.

## Running the Client

To start the client and connect to a server:

```
./kv_client [server_ip] [port]
```

If server_ip is not specified, 127.0.0.1 (localhost) will be used.
If port is not specified, the default port (8080) will be used.

## Using the Client

The client provides an interactive interface with the following commands:

- `PUT`: Store a key-value pair
- `GET`: Retrieve a value by key
- `DELETE`: Remove a key-value pair
- `LIST`: List all keys in the store
- `JOIN`: Add a node to the cluster
- `LEAVE`: Remove a node from the cluster
- `QUIT`: Exit the client

## Creating a Cluster

To create a cluster of nodes:

1. Start the first server: `./kv_server 8080`
2. Start additional servers on different ports: `./kv_server 8081`, `./kv_server 8082`, etc.
3. Connect a client to any server: `./kv_client 127.0.0.1 8080`
4. Use the JOIN command to register other nodes with the cluster

## Implementation Details

- **Consistent Hashing**: Keys are distributed among nodes using a hash function
- **Replication**: Data is replicated to other nodes when PUT/DELETE operations are performed
- **Thread Safety**: All operations are thread-safe using mutexes
- **Node Management**: Nodes can join and leave the cluster dynamically

## Limitations

This is a simplified implementation with some limitations:

- No persistence (data is only stored in memory)
- Limited error handling and recovery
- No automatic node discovery
- Simplified consistent hashing implementation
- No authentication or security features

## Project Structure

- `src/kv_store.h`: Main header file with data structures and function declarations
- `src/kv_store.c`: Implementation of the core key-value store functionality
- `src/kv_server.c`: Server implementation
- `src/kv_client.c`: Client implementation and interactive interface
- `Makefile`: Build configuration
