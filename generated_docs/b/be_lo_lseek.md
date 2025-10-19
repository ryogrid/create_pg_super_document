# be_lo_lseek

## Location
[src/backend/libpq/be-fsstubs.c:206-230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L206-L230)

## Overview
Seeks to a specific position within a PostgreSQL large object, similar to the standard C library lseek function, and returns the new absolute position.

## Definition
```c
Datum be_lo_lseek(PG_FUNCTION_ARGS)
```

## Detailed Description
The `be_lo_lseek` function provides seeking functionality for PostgreSQL large objects. It changes the current position within a large object based on the provided offset and whence parameter (similar to SEEK_SET, SEEK_CUR, SEEK_END). The function validates the file descriptor, calls the underlying `inv_seek` function, and performs overflow checking to ensure the result fits within the expected integer range before returning the new position.

## Parameters / Member Variables
- `PG_GETARG_INT32(0)`: File descriptor of the large object
- `PG_GETARG_INT32(1)`: Offset value for seeking
- `PG_GETARG_INT32(2)`: Whence parameter (origin for the offset)

## Dependencies
- Functions called/Symbols referenced:
  - [inv_seek](../i/inv_seek.md)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Returns the new absolute position as an INT32 Datum
- Performs overflow checking to ensure the 64-bit result from inv_seek fits in 32-bit return value
- Reports ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE if the position exceeds 32-bit integer limits
- Validates file descriptor similar to other large object functions
- Part of PostgreSQL's SQL-accessible large object API
- The whence parameter follows standard lseek conventions (SEEK_SET=0, SEEK_CUR=1, SEEK_END=2)

## Simplified Source

```c
Datum
be_lo_lseek(PG_FUNCTION_ARGS)
{
    int32 fd = PG_GETARG_INT32(0);
    int32 offset = PG_GETARG_INT32(1);
    int32 whence = PG_GETARG_INT32(2);
    int64 status;

    // Validate file descriptor
    if (fd < 0 || fd >= cookies_size || cookies[fd] == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("invalid large-object descriptor: %d", fd)));

    // Perform seek operation
    status = inv_seek(cookies[fd], offset, whence);

    // Check for overflow when converting to 32-bit result
    if (status != (int32) status)
        ereport(ERROR,
                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("lo_lseek result out of range for large-object descriptor %d", fd)));

    PG_RETURN_INT32((int32) status);
}
```