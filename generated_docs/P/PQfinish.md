# PQfinish

## Location
src/interfaces/libpq/fe-connect.c: 4878 - 4891

## Overview
Properly closes a connection to the PostgreSQL backend and frees the PGconn data structure, making it unusable after this call.

## Definition
```c
void PQfinish(PGconn *conn)
```

## Detailed Description
The `PQfinish` function is the primary public API for completely terminating a PostgreSQL connection and deallocating all associated resources. It performs a two-step cleanup process: first calling `pqClosePGconn` to properly close the connection and reset transient state, then calling `freePGconn` to deallocate the PGconn data structure itself. After this function returns, the connection pointer should not be used again as the memory has been freed. The function includes a null-pointer check to safely handle cases where a NULL connection is passed.

## Parameters / Member Variables
- `conn`: Pointer to the PostgreSQL connection object (PGconn) to be finished and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - pqClosePGconn
  - freePGconn
- Called from (representative examples):
  - libpqrcv_disconnect (WAL receiver)
  - disconnect_atexit (pg_basebackup)
  - DisconnectDatabase (pg_dump)
  - connectToServer (pg_upgrade)
  - finishCon (pgbench)
  - do_connect (psql)
  - PQcancelFinish
  - exit_nicely (various test programs)

## Notes and Other Information
- This is the standard function for properly cleaning up PostgreSQL connections
- The function safely handles NULL pointers by checking before proceeding
- After calling this function, the PGconn pointer becomes invalid and should not be used
- Used extensively throughout PostgreSQL client applications and utilities
- Part of the public libpq API exposed to client applications
- Essential for preventing memory leaks in PostgreSQL client programs
- Should be called for every successful PQconnectdb/PQconnectStart result
- Often called in error handling paths and application cleanup routines