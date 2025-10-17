# pg_snapshot_send

## Location
[src/backend/utils/adt/xid8funcs.c:534-554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L534-L554)

## Overview
Binary output function for the pg_snapshot data type that serializes a pg_snapshot structure into binary format for network transmission or storage.

## Definition
```c
Datum pg_snapshot_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The pg_snapshot_send function serves as the binary output conversion function for PostgreSQL's pg_snapshot data type. It takes a pg_snapshot structure and converts it into a binary representation that can be efficiently transmitted over the network or stored in binary format. This function is part of PostgreSQL's binary protocol support, providing the counterpart to pg_snapshot_recv.

The binary format produced consists of:
1. int4 nxip - number of transaction IDs in the xip array
2. u64 xmin - minimum transaction ID (as unsigned 64-bit integer)
3. u64 xmax - maximum transaction ID (as unsigned 64-bit integer)  
4. u64 xip[] - array of active transaction IDs (nxip elements, each as unsigned 64-bit integer)

The function uses PostgreSQL's standard binary output infrastructure with pq_* functions to ensure proper byte ordering and format compatibility across different platforms and architectures.

## Parameters / Member Variables
- `snap`: A pg_snapshot structure containing the snapshot data to be serialized to binary format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARLENA_P
  - [pq_begintypsend](pq_begintypsend.md)
  - [pq_sendint32](pq_sendint32.md)
  - [pq_sendint64](pq_sendint64.md)
  - [pq_endtypsend](pq_endtypsend.md)
  - U64FromFullTransactionId
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - No direct references found in the analyzed codebase (typically called by PostgreSQL's binary protocol handlers)

## Notes and Other Information
- This function is part of PostgreSQL's binary protocol infrastructure for pg_snapshot
- Produces binary data in network byte order for cross-platform compatibility
- All transaction IDs are converted from FullTransactionId format to 64-bit unsigned integers for transmission
- The output format is designed to be efficiently parsed by pg_snapshot_recv
- Uses PostgreSQL's standard binary output functions (pq_begintypsend, pq_sendint*, pq_endtypsend) for consistent formatting
- Returns the binary data as a bytea (binary array) PostgreSQL data type
- The binary format is compact and efficient for network transmission compared to text representation
- Located in src/backend/utils/adt/xid8funcs.c:534-554

## Simplified Source

```c
Datum pg_snapshot_send(PG_FUNCTION_ARGS) {
    // Get snapshot structure from input argument
    pg_snapshot *snap = (pg_snapshot *) PG_GETARG_VARLENA_P(0);
    StringInfoData buf;

    // Initialize binary output buffer
    pq_begintypsend(&buf);

    // Send snapshot components in binary format:
    // 1. Number of active transaction IDs
    pq_sendint32(&buf, snap->nxip);

    // 2. Minimum and maximum transaction IDs (converted to 64-bit)
    pq_sendint64(&buf, (int64) U64FromFullTransactionId(snap->xmin));
    pq_sendint64(&buf, (int64) U64FromFullTransactionId(snap->xmax));

    // 3. Array of active transaction IDs
    for (uint32 i = 0; i < snap->nxip; i++) {
        pq_sendint64(&buf, (int64) U64FromFullTransactionId(snap->xip[i]));
    }

    // Return serialized binary data as bytea
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```