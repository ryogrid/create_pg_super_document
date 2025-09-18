# libpqrcv_get_senderinfo

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:420-443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L420-L443)

## Overview
Retrieves the hostname and port information of the PostgreSQL server (sender) that the WAL receiver is currently connected to.

## Definition
```c
static void libpqrcv_get_senderinfo(WalReceiverConn *conn, char **sender_host, int *sender_port)
```

## Detailed Description
The `libpqrcv_get_senderinfo` function extracts network connection information from an active WAL receiver connection to identify the source server (sender) in a replication setup. This information is useful for monitoring, logging, and administrative purposes where you need to know which primary server a standby is receiving WAL data from.

The function queries the underlying libpq connection to obtain the actual hostname and port that were resolved and used for the connection. This may differ from what was originally specified in the connection string due to hostname resolution, default port assignment, or connection redirects.

The function safely handles cases where host or port information is unavailable, setting appropriate default values.

## Parameters / Member Variables
- `conn`: Active WAL receiver connection from which to extract sender information (must not be NULL)
- `sender_host`: Output parameter for the sender's hostname (will be palloc'ed string or NULL if unavailable)
- `sender_port`: Output parameter for the sender's port number (will be 0 if unavailable)

## Dependencies
- Functions called/Symbols referenced:
  - [PQhost](../P/PQhost.md) (get hostname from libpq connection)
  - [PQport](../P/PQport.md) (get port from libpq connection)
  - [pstrdup](../p/pstrdup.md) (duplicate hostname string using PostgreSQL memory management)
  - `strlen` (check string length)
  - `atoi` (convert port string to integer)

- Called from (representative examples):
  - Registered in `PQWalReceiverFunctions` table as `walrcv_get_senderinfo`
  - Used by WAL receiver processes for status reporting
  - Called when displaying replication connection details in monitoring views

## Notes and Other Information
- Output parameters are always initialized (sender_host to NULL, sender_port to 0) before processing
- Returns palloc'ed string for sender_host that should be freed by caller if not NULL
- Safely handles missing or empty host/port information by setting defaults
- Function requires an active connection (conn->streamConn must not be NULL)
- Hostname returned is the resolved hostname, not necessarily what was in the connection string
- [Port](../P/Port.md) is converted from string to integer; invalid port strings will result in 0
- Useful for identifying the source server in master-standby replication topologies
- Information can be used for monitoring replication lag and connection health
- Does not perform any network operations; only queries existing connection metadata