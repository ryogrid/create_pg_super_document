# pg_snapshot_recv

## Location
src/backend/utils/adt/xid8funcs.c: 468 - 533

## Overview
Binary input function for the pg_snapshot data type that deserializes a binary representation of a snapshot from the network or storage format.

## Definition
```c
Datum pg_snapshot_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The pg_snapshot_recv function serves as the binary input conversion function for PostgreSQL's pg_snapshot data type. It reads binary data from a StringInfo buffer and reconstructs a pg_snapshot structure. This function is part of PostgreSQL's binary protocol support, allowing pg_snapshot values to be transmitted efficiently between client and server or stored in binary format.

The binary format consists of:
1. int4 nxip - number of transaction IDs in the xip array
2. int8 xmin - minimum transaction ID  
3. int8 xmax - maximum transaction ID
4. int8 xip[] - array of active transaction IDs (nxip elements)

The function performs extensive validation to ensure data integrity:
- Validates that nxip is within acceptable bounds (0 to PG_SNAPSHOT_MAX_NXIP)
- Ensures xmin and xmax are valid transaction IDs with xmin ≤ xmax
- Verifies that all transaction IDs in the xip array are in ascending order
- Checks that all xip values fall within the [xmin, xmax) range
- Automatically removes duplicate transaction IDs during processing

## Parameters / Member Variables
- `buf`: A StringInfo buffer containing the binary data to be deserialized

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - [pq_getmsgint](pq_getmsgint.md)
  - [pq_getmsgint64](pq_getmsgint64.md)
  - FullTransactionIdFromU64
  - FullTransactionIdIsValid
  - FullTransactionIdPrecedes
  - FullTransactionIdEquals
  - [palloc](palloc.md)
  - SET_VARSIZE
  - ereport
  - PG_SNAPSHOT_SIZE
  - PG_SNAPSHOT_MAX_NXIP
  - InvalidFullTransactionId
- Called from (representative examples):
  - No direct references found in the analyzed codebase (typically called by PostgreSQL's binary protocol handlers)

## Notes and Other Information
- This function is part of PostgreSQL's binary protocol infrastructure for pg_snapshot
- Performs comprehensive validation to prevent malformed or malicious binary data from corrupting the system
- Automatically handles duplicate transaction IDs by skipping them and adjusting the count
- Uses efficient binary message reading functions (pq_getmsgint, pq_getmsgint64) for network byte order handling
- Will throw an ERROR with ERRCODE_INVALID_BINARY_REPRESENTATION if the binary data is malformed
- The transaction IDs must be provided in ascending order in the binary format
- All transaction IDs in xip must fall within the range [xmin, xmax) for the snapshot to be valid
- Located in src/backend/utils/adt/xid8funcs.c:468-533