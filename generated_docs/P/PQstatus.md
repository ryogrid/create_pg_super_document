# PQstatus

## Location
[src/interfaces/libpq/fe-connect.c:7106-7113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7106-L7113)

## Overview
PQstatus returns the current connection status of a PostgreSQL database connection, indicating whether the connection is functional, failed, or in an intermediate state.

## Definition
```c
ConnStatusType PQstatus(const PGconn *conn)
```

## Detailed Description
This function returns the current status of a PostgreSQL connection as represented by the ConnStatusType enumeration. It provides essential information about whether a connection is ready for use, has failed, or is in one of the various intermediate connection states.

The function directly returns the status field from the PGconn structure, which is maintained throughout the connection lifetime and updated as the connection state changes. For invalid connection pointers, it returns CONNECTION_BAD as a safe fallback.

## Parameters / Member Variables
- `conn`: A pointer to a PGconn structure representing the database connection. If NULL, the function returns CONNECTION_BAD.

## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_BAD (ConnStatusType enum value)
  - PGTransactionStatusType (referenced in nearby code)
- Called from (representative examples):
  - libpqrcv_connect (replication/libpqwalreceiver)
  - GetConnection (streamutil.c)
  - ConnectDatabase (pg_dump utilities)
  - do_connect (psql)
  - ConnectionUp (psql)
  - connectDatabase (fe_utils)
  - ECPGconnect (ECPG interface)
  - Various test programs and utilities

## Notes and Other Information
- Returns CONNECTION_BAD for NULL connection pointers
- The ConnStatusType enum includes various states:
  - CONNECTION_OK: Connection is ready for queries
  - CONNECTION_BAD: Connection has failed or is invalid
  - Multiple intermediate states for non-blocking connections (CONNECTION_STARTED, CONNECTION_MADE, etc.)
- This is one of the most frequently used libpq functions for connection health checking
- Essential for error handling and connection state verification
- Used extensively throughout PostgreSQL utilities and client applications
- The returned status reflects the current state and may change during connection establishment