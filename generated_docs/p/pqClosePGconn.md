# pqClosePGconn

## Location
src/interfaces/libpq/fe-connect.c: 4832 - 4877

## Overview
Properly closes a connection to the PostgreSQL backend, resetting all transient state while preserving connection parameters for potential reconnection.

## Definition
```c
void pqClosePGconn(PGconn *conn)
```

## Detailed Description
The `pqClosePGconn` function performs a comprehensive shutdown of a PostgreSQL connection while preserving the connection parameters for potential reuse. It first attempts a graceful shutdown by sending a terminate message to the backend, then resets the connection to non-blocking mode to ensure reconnection compatibility. The function systematically cleans up all transient state including I/O buffers, error states, transaction status, pipeline status, and asynchronous results. It also releases address information (except for cancel requests to allow PQcancelReset functionality) and drops all server-specific data. After completion, the PGconn is in a clean state suitable for establishing a fresh connection with the same parameters.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object (PGconn) to be closed and reset

## Dependencies
- Functions called/Symbols referenced:
  - [sendTerminateConn](../s/sendTerminateConn.md)
  - [pqDropConnection](pqDropConnection.md)
  - CONNECTION_BAD (status constant)
  - PGASYNC_IDLE (async status constant)
  - PQTRANS_IDLE (transaction status constant)
  - PQ_PIPELINE_OFF (pipeline status constant)
  - [pqClearAsyncResult](pqClearAsyncResult.md)
  - pqClearConnErrorState
  - [release_conn_addrinfo](../r/release_conn_addrinfo.md)
  - [pqDropServerData](pqDropServerData.md)
- Called from (representative examples):
  - [PQcancelReset](../P/PQcancelReset.md)
  - [PQfinish](../P/PQfinish.md)
  - [PQreset](../P/PQreset.md)
  - [PQresetStart](../P/PQresetStart.md)

## Notes and Other Information
- This function is designed to be used as part of connection reset/cleanup operations
- Preserves connection parameters to enable reconnection with the same settings
- Sets blocking mode to false without using PQsetnonblocking() to avoid flush failures
- Clears error states to provide a clean slate for new connection attempts
- Special handling for cancel requests: preserves address information to support PQcancelReset
- After this function, the connection status is set to CONNECTION_BAD but the connection can be reestablished
- Part of the libpq connection lifecycle management infrastructure
- Used internally by higher-level functions like PQfinish and PQreset