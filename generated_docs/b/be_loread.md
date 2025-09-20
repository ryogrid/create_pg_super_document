# be_loread

## Location
[src/backend/libpq/be-fsstubs.c:357-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L357-L374)

## Overview
Reads data from a large object and returns it as a bytea (binary data) value.

## Definition

```c
Datum
be_loread(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the backend functionality for reading data from large objects in PostgreSQL. It takes a file descriptor and length parameter, reads the specified amount of data from the large object, and returns it as a PostgreSQL bytea data type.

The function performs the following operations:
1. **Parameter validation**: Ensures the length parameter is not negative (sets to 0 if negative)
2. **Memory allocation**: Allocates memory for the return bytea structure including the VARHDRSZ header
3. **Data reading**: Calls the low-level  function to read data from the large object
4. **Size setting**: Sets the actual size of the returned bytea based on bytes actually read
5. **Return**: Returns the populated bytea structure

This function handles the interface between PostgreSQL's function calling mechanism and the large object storage system.

## Parameters / Member Variables
-  (int32): File descriptor of the open large object, obtained from 
-  (int32): Number of bytes to read from the large object, obtained from 
-  (bytea*): Allocated bytea structure to hold the read data
-  (int): Actual number of bytes read from the large object

## Dependencies
- Functions called/Symbols referenced:
  - [lo_read](../l/lo_read.md)
  - VARDATA (macro)
  - SET_VARSIZE (macro)
  - PG_RETURN_BYTEA_P (macro)
- Called from (representative examples):
  - No direct references found (likely called through function manager)

## Notes and Other Information
- Negative length parameters are automatically converted to 0 for safety
- The function allocates memory that includes PostgreSQL's variable-length data header (VARHDRSZ)
- The actual amount of data read may be less than requested if the large object doesn't contain enough data
- The returned bytea size is set to match the actual bytes read, not the requested length
- Located in 
- Part of the Read/Write using bytea section of the large object implementation