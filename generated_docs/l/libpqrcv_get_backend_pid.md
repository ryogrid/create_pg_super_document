# libpqrcv_get_backend_pid

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1150-1158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L1150-L1158)

## Overview
libpqrcv_get_backend_pid retrieves the process ID (PID) of the backend process on the primary server that is handling the current WAL receiver streaming connection.

## Definition
```c
static pid_t libpqrcv_get_backend_pid(WalReceiverConn *conn)
```

## Detailed Description
This function serves as a simple wrapper around libpq's PQbackendPID function to obtain the process ID of the remote backend process that is serving the streaming replication connection. The PID can be useful for monitoring, debugging, and administrative purposes, such as identifying which specific backend process is handling the replication stream on the primary server. This information is particularly valuable in troubleshooting replication issues or monitoring the health of individual replication connections in a multi-standby environment.

## Parameters / Member Variables
- `conn`: Pointer to WalReceiverConn structure containing the active streaming connection to the primary server

## Dependencies
- Functions called/Symbols referenced:
  - [PQbackendPID](../P/PQbackendPID.md): libpq function that returns the process ID of the backend serving the connection
- Called from (representative examples):
  - Referenced by WalReceiverConn structure function pointers
  - Used by monitoring and administrative functions that need to identify the remote backend process

## Notes and Other Information
- This is a static function, accessible only within the libpqwalreceiver.c compilation unit
- The function is a thin wrapper with no error handling - it directly passes through the result from PQbackendPID
- The returned PID corresponds to a process running on the primary server, not the local standby
- Useful for correlating replication connections with specific backend processes in server logs
- The PID can be used with system monitoring tools or PostgreSQL's pg_stat_activity view
- Returns pid_t type, which is typically an integer type representing process IDs
- Location: src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1150-1158

## Simplified Source

```c
static pid_t libpqrcv_get_backend_pid(WalReceiverConn *conn) {
    // Simple wrapper around libpq function to get remote backend PID
    return PQbackendPID(conn->streamConn);
}
```