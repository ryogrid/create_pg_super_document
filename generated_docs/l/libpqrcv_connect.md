# libpqrcv_connect

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 143 - 315

## Overview
Establishes a connection to a PostgreSQL primary server for WAL streaming replication, supporting both logical and physical replication modes.

## Definition
```c
static WalReceiverConn *libpqrcv_connect(const char *conninfo, bool replication, bool logical,
                                       bool must_use_password, const char *appname, char **err)
```

## Detailed Description
The `libpqrcv_connect` function establishes a PostgreSQL connection specifically for WAL (Write-Ahead Log) streaming replication purposes. It can handle both regular database connections and replication connections (logical or physical). The function implements asynchronous connection establishment using libpq's polling mechanism to avoid blocking the server process.

For logical replication, the function configures specific client settings including encoding translation and GUC parameters to ensure consistent data interpretation between publisher and subscriber. For physical replication, it sets up a replication-mode connection.

The function includes robust error handling and security measures, including password validation for non-superusers and secure search path configuration to prevent malicious SQL injection.

## Parameters / Member Variables
- `conninfo`: Connection string or URI specifying the target PostgreSQL server
- `replication`: Whether this is a replication connection (true) or regular connection (false)
- `logical`: For replication connections, whether it's logical (true) or physical (false) replication
- `must_use_password`: If true, connection must use password authentication (security requirement)
- `appname`: Application name to identify the connection in server logs and statistics
- `err`: Output parameter for error messages (palloc'ed string on failure)

## Dependencies
- Functions called/Symbols referenced:
  - [libpqrcv_check_conninfo](libpqrcv_check_conninfo.md) (validate connection parameters)
  - [GetDatabaseEncodingName](../G/GetDatabaseEncodingName.md) (for logical replication encoding)
  - [PQconnectStartParams](../P/PQconnectStartParams.md) (initiate asynchronous connection)
  - [PQconnectPoll](../P/PQconnectPoll.md) (advance connection state machine)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md) (wait for socket events)
  - [ProcessWalRcvInterrupts](../P/ProcessWalRcvInterrupts.md) (handle interrupts during connection)
  - [libpqrcv_PQexec](libpqrcv_PQexec.md) (execute secure search path SQL)
  - Various libpq functions (PQstatus, PQconnectionUsedPassword, etc.)

- Called from (representative examples):
  - Registered in `PQWalReceiverFunctions` table as `walrcv_connect`
  - Used by WAL receiver processes for establishing replication connections
  - Invoked by logical replication workers

## Notes and Other Information
- Uses asynchronous connection establishment to prevent blocking the server process
- Implements special handling for logical replication including encoding and GUC parameter setup
- Enforces password authentication requirements for security when `must_use_password` is true
- Sets secure search path for connections that will execute SQL queries
- Connection polling respects process interrupts and latch signals
- Returns NULL on failure with error message in `err` parameter
- May call ereport(ERROR) for password-related security violations instead of returning NULL
- Supports both database connections and replication protocol connections