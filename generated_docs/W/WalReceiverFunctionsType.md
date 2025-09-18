# WalReceiverFunctionsType

## Location
src/include/replication/walreceiver.h: 409 - 428

## Overview
The WalReceiverFunctionsType structure defines a function pointer interface for WAL (Write-Ahead Log) receiver operations, providing a pluggable architecture for different WAL receiver implementations in PostgreSQL's replication system.

## Definition
```c
typedef struct WalReceiverFunctionsType
{
    walrcv_connect_fn walrcv_connect;
    walrcv_check_conninfo_fn walrcv_check_conninfo;
    walrcv_get_conninfo_fn walrcv_get_conninfo;
    walrcv_get_senderinfo_fn walrcv_get_senderinfo;
    walrcv_identify_system_fn walrcv_identify_system;
    walrcv_get_dbname_from_conninfo_fn walrcv_get_dbname_from_conninfo;
    walrcv_server_version_fn walrcv_server_version;
    walrcv_readtimelinehistoryfile_fn walrcv_readtimelinehistoryfile;
    walrcv_startstreaming_fn walrcv_startstreaming;
    walrcv_endstreaming_fn walrcv_endstreaming;
    walrcv_receive_fn walrcv_receive;
    walrcv_send_fn walrcv_send;
    walrcv_create_slot_fn walrcv_create_slot;
    walrcv_alter_slot_fn walrcv_alter_slot;
    walrcv_get_backend_pid_fn walrcv_get_backend_pid;
    walrcv_exec_fn walrcv_exec;
    walrcv_disconnect_fn walrcv_disconnect;
} WalReceiverFunctionsType;
```

## Detailed Description
WalReceiverFunctionsType serves as a comprehensive function pointer table that defines the complete interface for WAL receiver operations in PostgreSQL's replication subsystem. This structure implements a pluggable architecture pattern, allowing different WAL receiver implementations (such as libpqwalreceiver) to provide their own implementation of the core replication functions while maintaining a consistent interface.

The structure encapsulates all the essential operations needed for establishing replication connections, managing WAL streaming, handling replication slots, and executing commands on remote PostgreSQL instances. This design enables PostgreSQL to support multiple replication transport mechanisms through a unified interface, promoting modularity and extensibility in the replication system.

## Parameters / Member Variables
- `walrcv_connect`: Function pointer for establishing connections to remote clusters (both replication and regular connections)
- `walrcv_check_conninfo`: Function pointer for parsing and validating connection strings
- `walrcv_get_conninfo`: Function pointer for retrieving user-displayable connection information with sensitive fields obfuscated
- `walrcv_get_senderinfo`: Function pointer for obtaining WAL sender information (host and port)
- `walrcv_identify_system`: Function pointer for running IDENTIFY_SYSTEM command and validating cluster identity
- `walrcv_get_dbname_from_conninfo`: Function pointer for extracting database name from connection info
- `walrcv_server_version`: Function pointer for retrieving the version number of the connected cluster
- `walrcv_readtimelinehistoryfile`: Function pointer for fetching timeline history files from the cluster
- `walrcv_startstreaming`: Function pointer for initiating WAL data streaming with specified options
- `walrcv_endstreaming`: Function pointer for stopping WAL data streaming and retrieving next timeline ID
- `walrcv_receive`: Function pointer for receiving messages from the WAL stream
- `walrcv_send`: Function pointer for sending messages to the WAL stream
- `walrcv_create_slot`: Function pointer for creating new replication slots (both logical and physical)
- `walrcv_alter_slot`: Function pointer for modifying replication slot properties (currently supports failover property)
- `walrcv_get_backend_pid`: Function pointer for retrieving the PID of the remote backend process
- `walrcv_exec`: Function pointer for sending generic queries and commands to the remote cluster, returning WalRcvExecResult
- `walrcv_disconnect`: Function pointer for disconnecting from the cluster

## Dependencies
- Functions called/Symbols referenced:
  - walrcv_connect_fn
  - walrcv_check_conninfo_fn
  - walrcv_get_conninfo_fn
  - walrcv_get_senderinfo_fn
  - walrcv_identify_system_fn
  - walrcv_get_dbname_from_conninfo_fn
  - walrcv_server_version_fn
  - walrcv_readtimelinehistoryfile_fn
  - walrcv_startstreaming_fn
  - walrcv_endstreaming_fn
  - walrcv_receive_fn
  - walrcv_send_fn
  - walrcv_create_slot_fn
  - walrcv_alter_slot_fn
  - walrcv_get_backend_pid_fn
  - walrcv_exec_fn
  - walrcv_disconnect_fn

- Called from (representative examples):
  - WalReceiverConn (src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:91)

## Notes and Other Information
- This structure implements the Strategy pattern, allowing different WAL receiver implementations to be plugged into the system
- The libpqwalreceiver module is the primary implementation of this interface, providing PostgreSQL's standard replication functionality
- All function pointers must be properly initialized before use, typically done during WAL receiver module initialization
- The interface supports both physical and logical replication through various function combinations
- Error handling and connection management are delegated to the specific implementation behind each function pointer
- The structure is central to PostgreSQL's distributed architecture, enabling features like streaming replication, logical replication, and subscription management
- This design allows for potential future replication transport implementations beyond libpq