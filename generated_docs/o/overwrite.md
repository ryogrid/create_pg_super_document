# overwrite

## Location
[src/test/examples/testlo64.c:113-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/examples/testlo64.c#L113-L150)

## Overview
A static function that overwrites a specified portion of a PostgreSQL large object with a pattern of 'X' characters starting at a given offset.

## Definition

```c
static void
overwrite(PGconn *conn, Oid lobjId, pg_int64 start, int len)
```
## Detailed Description
The  function provides functionality to overwrite a specific segment of data in a PostgreSQL large object. It opens the large object in write mode, seeks to the specified starting position, creates a buffer filled with 'X' characters, and writes this pattern to the large object. The function handles partial writes by continuing to write until the entire buffer has been written or an error occurs.

## Parameters / Member Variables
- `*conn`: Database connection handle for PostgreSQL operations
- `lobjId`: OID of the large object to write to
- `start`: Starting byte position within the large object where overwriting begins
- `len`: Number of bytes to overwrite with the 'X' pattern
## Dependencies
- Functions called/Symbols referenced:
  - [lo_open](../l/lo_open.md) (PostgreSQL large object opening)
  - [lo_lseek](../l/lo_lseek.md) (PostgreSQL large object seeking)
  - [lo_write](../l/lo_write.md) (PostgreSQL large object writing)
  - [lo_close](../l/lo_close.md) (PostgreSQL large object closing)
  - malloc (memory allocation)
  - free (memory deallocation)
  - fprintf (formatted output to stderr)
  - INV_WRITE (large object access mode constant)
  - SEEK_SET (seek position constant)
- Called from (representative examples):
  - [main](../m/main.md) (in src/test/examples/testlo.c:258)
  - [main](../m/main.md) (in src/test/examples/testlo64.c:282)

## Notes and Other Information
- This is a static function used in PostgreSQL test examples for demonstrating large object write operations
- Fills the entire buffer with 'X' characters before writing, creating a distinctive overwrite pattern
- Uses malloc to allocate a buffer of the requested length plus one byte for null termination
- Handles partial writes by continuing to write from the correct buffer offset until completion
- Includes error handling for write failures with "WRITE FAILED!" message to stderr
- Properly manages memory by freeing the allocated buffer after use
- The function will break out of the write loop if a write operation fails (nbytes <= 0)
- Used primarily for testing and demonstrating large object modification capabilities