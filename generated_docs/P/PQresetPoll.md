# PQresetPoll

## Location
src/interfaces/libpq/fe-connect.c: 4944 - 4985

## Overview
Continues the asynchronous reset process of a PostgreSQL database connection initiated by PQresetStart, returning the current status of the reset operation.

## Definition
```c
PostgresPollingStatusType PQresetPoll(PGconn *conn)
```

## Detailed Description
PQresetPoll is the polling counterpart to PQresetStart in libpq's asynchronous connection reset interface. This function should be called repeatedly after PQresetStart to complete the connection reset process without blocking the application. It internally uses PQconnectPoll to handle the actual connection establishment phases and monitors the progress of the reset operation.

When the reset completes successfully (PGRES_POLLING_OK), the function notifies all registered event procedures about the successful connection reset by sending PGEVT_CONNRESET events. This allows applications and libraries to perform cleanup or reinitialization tasks when a connection is reset.

## Parameters / Member Variables
- `conn`: A pointer to the PGconn structure representing the PostgreSQL connection being reset. If NULL, the function returns PGRES_POLLING_FAILED immediately.

## Dependencies
- Functions called/Symbols referenced:
  - [PQconnectPoll](PQconnectPoll.md)
  - PostgresPollingStatusType
  - PGRES_POLLING_OK
  - [PGEventConnReset](PGEventConnReset.md)
  - PGEVT_CONNRESET
  - PGRES_POLLING_FAILED
- Called from (representative examples):
  - PQsetdb (referenced in libpq-fe.h)

## Notes and Other Information
- Returns PostgresPollingStatusType indicating the current status of the reset operation:
  - PGRES_POLLING_READING: Waiting for socket to be readable
  - PGRES_POLLING_WRITING: Waiting for socket to be writable
  - PGRES_POLLING_OK: Reset completed successfully
  - PGRES_POLLING_FAILED: Reset failed
- Must be called after PQresetStart to complete the asynchronous reset process
- Triggers PGEVT_CONNRESET events to all registered event procedures upon successful reset
- This is part of libpq's public API for non-blocking database operations
- The function preserves and reuses the original connection parameters during reset