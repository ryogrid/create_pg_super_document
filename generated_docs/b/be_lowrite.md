# be_lowrite

## Location
[src/backend/libpq/be-fsstubs.c:375-397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L375-L397)

## Overview
Writes binary data from a bytea value to a large object and returns the number of bytes written.

## Definition

```c
Datum
be_lowrite(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the backend functionality for writing data to large objects in PostgreSQL. It takes a file descriptor and a bytea containing the data to write, performs the write operation, and returns the number of bytes actually written.

The function performs the following operations:
1. **Parameter extraction**: Gets the file descriptor and bytea data from function arguments
2. **Read-only protection**: Prevents write operations in read-only transactions
3. **Size calculation**: Determines the number of bytes to write from the bytea header
4. **Data writing**: Calls the low-level  function to write data to the large object
5. **Return count**: Returns the actual number of bytes written

This function serves as the interface between PostgreSQL's SQL  function and the internal large object storage system.

## Parameters / Member Variables
-  (int32): File descriptor of the open large object, obtained from 
-  (bytea*): The bytea structure containing data to write, obtained from 
-  (int): Number of bytes to write, calculated from the bytea size
-  (int): Actual number of bytes written to the large object

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (macro)
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)
  - [lo_write](../l/lo_write.md)
  - VARSIZE_ANY_EXHDR (macro)
  - VARDATA_ANY (macro)
- Called from (representative examples):
  - No direct references found (likely called through function manager)

## Notes and Other Information
- The function enforces read-only transaction protection, preventing writes in read-only contexts
- Uses  for efficient handling of potentially compressed/external bytea values
- The actual bytes written may be less than requested if storage constraints are encountered
-  macro calculates the data size excluding the variable-length header
-  macro gets a pointer to the actual data portion of the bytea
- Located in src/backend/libpq/be-fsstubs.c:375-397

## Simplified Source

```c
Datum be_lowrite(PG_FUNCTION_ARGS) {
    int32 fd = PG_GETARG_INT32(0);
    bytea *wbuf = PG_GETARG_BYTEA_PP(1);

    // Prevent write operations in read-only transactions
    PreventCommandIfReadOnly("lowrite()");

    // Calculate bytes to write and perform the write operation
    int bytestowrite = VARSIZE_ANY_EXHDR(wbuf);
    int totalwritten = lo_write(fd, VARDATA_ANY(wbuf), bytestowrite);

    PG_RETURN_INT32(totalwritten);
}
``` 
- Part of the Read/Write using bytea section of the large object implementation