# be_lo_put

## Location
[src/backend/libpq/be-fsstubs.c:850-868](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L850-L868)

## Overview
A PostgreSQL backend function that updates a specific range within an existing large object by writing bytea data at a specified offset position.

## Definition

```c
Datum
be_lo_put(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides selective writing capability for PostgreSQL large objects by allowing updates to specific regions within an existing large object. Unlike functions that create new large objects, this function modifies existing ones by seeking to a specified offset position and writing the provided bytea data. The function handles the complete update process: opening the large object for writing, seeking to the correct position, writing the data, and ensuring data integrity through assertion checking. This functionality is essential for efficient partial updates of large objects without requiring complete replacement.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  -  (Oid): The object identifier of the existing large object to update
  -  (int64): The starting position in bytes where the update should begin
  -  (bytea*): The bytea data to write to the large object at the specified offset

## Dependencies
- Functions called/Symbols referenced:
  - : Opens the large object for write access
  - : Positions the large object cursor at the specified offset
  - : Writes data to the large object at current cursor position
  - : Closes the large object descriptor
  - : Ensures operation is not executed in read-only transactions
  - : Macro to extract OID argument
  - : Macro to extract 64-bit integer argument (offset)
  - : Macro to extract bytea argument with possible detoasting
  - : Macro to return void result
  - : Macro to get data portion of bytea
  - : Macro to get size of bytea excluding header
  - : Constant indicating write access mode
  - : Standard constant for absolute positioning
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Modifies existing large objects without creating new ones
- Supports writing at any valid offset position within the large object
- Prevents execution in read-only transactions for data integrity
- Sets  flag for proper resource management
- Uses assertion checking to verify complete data transfer
- Can be used for both partial updates and appending data (depending on offset)
- Part of PostgreSQL's large object filesystem stub interface
- Located in src/backend/libpq/be-fsstubs.c:850-868

## Simplified Source

```c
Datum be_lo_put(PG_FUNCTION_ARGS) {
    Oid loOid = PG_GETARG_OID(0);
    int64 offset = PG_GETARG_INT64(1);
    bytea *str = PG_GETARG_BYTEA_PP(2);

    // Prevent operation in read-only transactions
    PreventCommandIfReadOnly("lo_put()");

    // Set cleanup flag and open large object for writing
    lo_cleanup_needed = true;
    LargeObjectDesc *loDesc = inv_open(loOid, INV_WRITE, CurrentMemoryContext);

    // Seek to position and write data
    inv_seek(loDesc, offset, SEEK_SET);
    int written = inv_write(loDesc, VARDATA_ANY(str), VARSIZE_ANY_EXHDR(str));
    Assert(written == VARSIZE_ANY_EXHDR(str));

    // Close and return
    inv_close(loDesc);
    PG_RETURN_VOID();
}
```