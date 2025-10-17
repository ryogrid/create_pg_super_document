# buf_add_txid

## Location
[src/backend/utils/adt/xid8funcs.c:237-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L237-L247)

## Overview
A static utility function that appends a FullTransactionId to a StringInfo buffer containing a pg_snapshot structure.

## Definition

```c
static void
buf_add_txid(StringInfo buf, FullTransactionId fxid)
```
## Detailed Description
This function is responsible for adding a transaction ID to a snapshot buffer during snapshot parsing operations. It performs two key actions: first, it increments the transaction count (nxip) in the pg_snapshot structure stored in the buffer, then appends the FullTransactionId in binary format to the buffer. The function includes a comment noting that the nxip increment is done before any potential reallocation that might occur during the append operation.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing a pg_snapshot structure where the transaction ID will be added
- `fxid`: FullTransactionId to be appended to the buffer in binary format
## Dependencies
- Functions called/Symbols referenced:
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md)
- Types referenced:
  - [FullTransactionId](../F/FullTransactionId.md)
  - [pg_snapshot](../p/pg_snapshot.md)
- Called from (representative examples):
  - [parse_snapshot](../p/parse_snapshot.md)

## Notes and Other Information
- This is a static function local to src/backend/utils/adt/xid8funcs.c
- The function assumes the buffer contains a valid pg_snapshot structure
- The nxip counter is incremented before the append operation to avoid potential issues with buffer reallocation
- The FullTransactionId is stored in binary format for efficient storage and retrieval

## Simplified Source

```c
static void buf_add_txid(StringInfo buf, FullTransactionId fxid) {
    // Get pointer to snapshot structure in buffer
    pg_snapshot *snap = (pg_snapshot *) buf->data;

    // Increment count of in-progress transactions
    // (done before reallocation to avoid pointer invalidation)
    snap->nxip++;

    // Append transaction ID in binary format to buffer
    appendBinaryStringInfo(buf, &fxid, sizeof(fxid));
}
```