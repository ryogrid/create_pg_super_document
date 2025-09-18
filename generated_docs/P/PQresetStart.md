# PQresetStart

## Location
[src/interfaces/libpq/fe-connect.c:4925-4943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L4925-L4943)

## Overview
Initiates an asynchronous reset of a PostgreSQL database connection, closing the existing connection and starting the process to establish a new one.

## Definition
```c
int PQresetStart(PGconn *conn)
```

## Detailed Description
PQresetStart is part of libpq's asynchronous connection interface that allows applications to reset a database connection without blocking. This function performs the initial phase of a connection reset by first closing the existing connection using pqClosePGconn, then starting the connection establishment process using pqConnectDBStart. The reset process allows a connection to be re-established with the same parameters that were used for the original connection, which is useful when recovering from connection errors or when a connection needs to be refreshed.

The function is designed to work in conjunction with PQresetPoll to provide non-blocking connection reset capabilities. After calling PQresetStart, the application should repeatedly call PQresetPoll until the reset operation completes.

## Parameters / Member Variables
- `conn`: A pointer to the PGconn structure representing the PostgreSQL connection to be reset. If NULL, the function returns 0 immediately.

## Dependencies
- Functions called/Symbols referenced:
  - [pqClosePGconn](../p/pqClosePGconn.md)
  - [pqConnectDBStart](../p/pqConnectDBStart.md)
  - PostgresPollingStatusType
- Called from (representative examples):
  - PQsetdb (referenced in libpq-fe.h)

## Notes and Other Information
- Returns 1 on successful initiation of the reset process, 0 on failure or if conn is NULL
- This is an asynchronous operation; use PQresetPoll to complete the reset process
- The connection parameters from the original connection are preserved and reused
- The function is part of libpq's public API for non-blocking database operations
- After calling this function, the connection should be considered invalid until the reset completes successfully