# be_lo_open

## Location
[src/backend/libpq/be-fsstubs.c:87-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L87-L125)

## Overview
Opens a PostgreSQL large object and returns a file descriptor that can be used for subsequent read/write operations on the large object.

## Definition

```c
Datum
be_lo_open(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL backend function that opens a large object (LO) for read/write operations. It takes an OID identifying the large object and an access mode, then returns a file descriptor that can be used for subsequent operations. The function handles transaction context management, snapshot registration, and access permission checks. It creates a LargeObjectDesc structure to track the opened large object and associates it with a file descriptor in the global cookies array.

## Parameters / Member Variables
- : Large object OID to open
- : Access mode flags (INV_READ, INV_WRITE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [LargeObjectDesc](../L/LargeObjectDesc.md)
  - DEBUG4
  - INV_WRITE
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)
  - [newLOfd](../n/newLOfd.md)
  - [inv_open](../i/inv_open.md)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md)
  - [RegisterSnapshotOnOwner](../R/RegisterSnapshotOnOwner.md)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Performs read-only transaction checks when opened with INV_WRITE mode
- Manages transaction-level resource ownership for snapshots
- Creates filesystem context (fscxt) if this is the first LO opened in the transaction
- Returns a PostgreSQL Datum containing the file descriptor as INT32
- Includes debug logging when FSDB is defined

## Simplified Source

```c
Datum
be_lo_open(PG_FUNCTION_ARGS)
{
    Oid         lobjId = PG_GETARG_OID(0);
    int32       mode = PG_GETARG_INT32(1);
    LargeObjectDesc *lobjDesc;
    int         fd;

    // Check write permission in read-only transactions
    if (mode & INV_WRITE)
        PreventCommandIfReadOnly("lo_open(INV_WRITE)");

    // Allocate a new file descriptor
    // This also creates 'fscxt' if this is the first LO opened in transaction
    fd = newLOfd();

    // Open the large object
    lobjDesc = inv_open(lobjId, mode, fscxt);
    lobjDesc->subid = GetCurrentSubTransactionId();

    // Register snapshot in TopTransaction's resource owner for proper lifecycle
    if (lobjDesc->snapshot)
        lobjDesc->snapshot = RegisterSnapshotOnOwner(lobjDesc->snapshot,
                                                     TopTransactionResourceOwner);

    // Associate descriptor with file descriptor
    Assert(cookies[fd] == NULL);
    cookies[fd] = lobjDesc;

    PG_RETURN_INT32(fd);
}
```