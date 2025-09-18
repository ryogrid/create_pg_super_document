# PQreset

## Location
src/interfaces/libpq/fe-connect.c: 4892 - 4924

## Overview
Resets the connection to the PostgreSQL backend by closing the existing connection and creating a new one with the same parameters.

## Definition
```c
void PQreset(PGconn *conn)
```

## Detailed Description
The `PQreset` function provides a way to reestablish a PostgreSQL connection using the same connection parameters as the original connection. It first calls `pqClosePGconn` to properly close the existing connection and clean up transient state, then attempts to reestablish the connection using `pqConnectDBStart` and `pqConnectDBComplete`. If the reconnection is successful, the function notifies all registered event procedures about the connection reset by sending PGEVT_CONNRESET events. This function is useful for recovering from connection failures or network interruptions while preserving the original connection configuration.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object (PGconn) to be reset and reconnected

## Dependencies
- Functions called/Symbols referenced:
  - [pqClosePGconn](../p/pqClosePGconn.md)
  - [pqConnectDBStart](../p/pqConnectDBStart.md)
  - [pqConnectDBComplete](../p/pqConnectDBComplete.md)
  - [PGEventConnReset](PGEventConnReset.md) (event structure)
  - PGEVT_CONNRESET (event type constant)
- Called from (representative examples):
  - [CheckConnection](../C/CheckConnection.md) (psql)
  - PQsetdb (compatibility function)

## Notes and Other Information
- This function preserves all connection parameters from the original connection
- The function safely handles NULL pointers by checking before proceeding
- If reconnection fails, the connection remains in a closed/bad state
- Event procedures are notified only upon successful reconnection
- Part of the public libpq API for connection recovery
- Useful for implementing connection retry logic in client applications
- The connection object remains valid but the underlying network connection is replaced
- All session-specific state (transactions, prepared statements, etc.) is lost during reset
- Applications should check the connection status after calling this function to verify success