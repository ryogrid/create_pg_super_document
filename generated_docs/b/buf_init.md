# buf_init

## Location
[src/backend/utils/adt/xid8funcs.c:222-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L222-L236)

## Overview
Initializes a StringInfo buffer with a basic pg_snapshot structure containing the specified xmin and xmax transaction IDs, used as a helper function for snapshot creation.

## Definition
```c
static StringInfo buf_init(FullTransactionId xmin, FullTransactionId xmax)
```

## Detailed Description
This function creates and initializes a StringInfo buffer containing a pg_snapshot structure with the provided minimum and maximum transaction IDs. It sets up the basic snapshot structure with xmin and xmax values, initializes nxip (number of in-progress transactions) to 0, and stores this snapshot structure in a binary StringInfo buffer.

The function uses makeStringInfo() to create a new StringInfo buffer and appendBinaryStringInfo() to write the binary representation of the snapshot structure. The PG_SNAPSHOT_SIZE(0) macro calculates the size needed for a snapshot with 0 in-progress transactions.

## Parameters / Member Variables
- `xmin`: The minimum transaction ID for the snapshot (oldest transaction still running when snapshot was taken)
- `xmax`: The maximum transaction ID for the snapshot (next transaction ID that will be assigned)

## Dependencies
- Functions called/Symbols referenced:
  - [makeStringInfo](../m/makeStringInfo.md)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
  - PG_SNAPSHOT_SIZE (macro)
  - [pg_snapshot](../p/pg_snapshot.md) (type)
  - [FullTransactionId](../F/FullTransactionId.md) (type)
- Called from (representative examples):
  - [parse_snapshot](../p/parse_snapshot.md)

## Notes and Other Information
- This is a static helper function used internally within the xid8funcs.c module
- Part of the infrastructure for creating and manipulating pg_snapshot structures
- Creates snapshots with no in-progress transactions initially (nxip = 0)
- The resulting StringInfo buffer contains a binary representation suitable for further processing
- Used in snapshot parsing and creation workflows

## Simplified Source

```c
static StringInfo buf_init(FullTransactionId xmin, FullTransactionId xmax) {
    // Create snapshot structure with basic transaction boundaries
    pg_snapshot snap;
    snap.xmin = xmin;  // Oldest active transaction
    snap.xmax = xmax;  // Next transaction ID to assign
    snap.nxip = 0;     // No in-progress transactions initially

    // Create StringInfo buffer and store snapshot as binary data
    StringInfo buf = makeStringInfo();
    appendBinaryStringInfo(buf, &snap, PG_SNAPSHOT_SIZE(0));

    return buf;
}
```