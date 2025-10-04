# be_lo_from_bytea

## Location
[src/backend/libpq/be-fsstubs.c:827-849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L827-L849)

## Overview
A PostgreSQL backend function that creates a new large object and initializes it with content from a provided bytea argument.

## Definition

```c
Datum
be_lo_from_bytea(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function creates a new large object in PostgreSQL and populates it with the complete contents of a bytea value. This function serves as a convenient way to convert regular PostgreSQL bytea data into large object storage. The function handles the entire lifecycle of large object creation: creating the object with a specified or system-generated OID, opening it for writing, writing the bytea content, and properly closing it. It includes read-only transaction protection and uses assertion checking to ensure data integrity during the write operation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  -  (Oid): The desired object identifier for the new large object (0 for system-generated OID)
  -  (bytea*): The bytea data to be written to the new large object

## Dependencies
- Functions called/Symbols referenced:
  - : Creates a new large object with specified or generated OID
  - : Opens the large object for access with specified mode
  - : Writes data to the large object
  - : Closes the large object descriptor
  - : Ensures operation is not executed in read-only transactions
  - : Macro to extract OID argument
  - : Macro to extract bytea argument with possible detoasting
  - : Macro to return the created large object's OID
  - : Macro to get data portion of bytea
  - : Macro to get size of bytea excluding header
  - : Constant indicating write access mode
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Creates large objects that can be accessed later via their returned OID
- Prevents execution in read-only transactions for data integrity
- Sets  flag for proper resource management
- Uses assertion checking to verify complete data transfer
- Efficiently handles bytea data using PostgreSQL's TOAST-aware macros
- Part of PostgreSQL's large object filesystem stub interface
- Located in src/backend/libpq/be-fsstubs.c:827-849

## Simplified Source

```c
Datum be_lo_from_bytea(PG_FUNCTION_ARGS) {
    Oid loOid = PG_GETARG_OID(0);
    bytea *str = PG_GETARG_BYTEA_PP(1);

    // Prevent operation in read-only transactions
    PreventCommandIfReadOnly("lo_from_bytea()");

    // Set cleanup flag and create large object
    lo_cleanup_needed = true;
    loOid = inv_create(loOid);

    // Open, write data, and close large object
    LargeObjectDesc *loDesc = inv_open(loOid, INV_WRITE, CurrentMemoryContext);
    int written = inv_write(loDesc, VARDATA_ANY(str), VARSIZE_ANY_EXHDR(str));
    Assert(written == VARSIZE_ANY_EXHDR(str));
    inv_close(loDesc);

    PG_RETURN_OID(loOid);
}
```