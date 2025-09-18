# pqCopyPGconn

## Location
src/interfaces/libpq/fe-connect.c: 956 - 996

## Overview
Copies connection option values from a source PGconn structure to a destination PGconn structure, handling memory allocation and deallocation for string fields.

## Definition
```c
bool pqCopyPGconn(PGconn *srcConn, PGconn *dstConn)
```

## Detailed Description
This function iterates through all connection options defined in PQconninfoOptions and copies their values from srcConn to dstConn. For each option that has a valid connection offset (connofs >= 0), it performs a deep copy of string values using strdup(). The function properly manages memory by freeing existing values in the destination before assigning new ones. The function is designed to be simple and straightforward, with the comment noting that intelligence should be in connectOptions2.

## Parameters / Member Variables
- `srcConn`: Source PGconn structure from which to copy connection options
- `dstConn`: Destination PGconn structure that will receive the copied options

## Dependencies
- Functions called/Symbols referenced:
  - PQconninfoOptions (global array of connection options)
  - internalPQconninfoOption (structure type)
  - strdup (standard library function for string duplication)
  - free (standard library function for memory deallocation)
  - libpq_append_conn_error (for error reporting)
- Called from (representative examples):
  - PQcancelCreate

## Notes and Other Information
- Returns true on success, false on failure
- On failure, sets an error message in the destination connection using libpq_append_conn_error
- Only copies options that have a valid connection offset (connofs >= 0)
- Performs deep copying of string values to avoid shared memory issues
- Handles memory management by freeing existing destination values before copying new ones
- Location: src/interfaces/libpq/fe-connect.c:956-996