# libpq_get_current_wal_insert_lsn

## Location
[src/bin/pg_rewind/libpq_source.c:209-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L209-L232)

## Overview
Retrieves the current WAL (Write-Ahead Log) insert LSN (Log Sequence Number) from a remote PostgreSQL server using the pg_current_wal_insert_lsn() function.

## Definition

```c
static XLogRecPtr
libpq_get_current_wal_insert_lsn(rewind_source *source)
```
## Detailed Description
The  function queries a remote PostgreSQL server to obtain the current WAL insert position. This function is part of the rewind_source interface and provides a way for pg_rewind to determine the exact point in the WAL stream where new records are being inserted on the source server.

The function works by:
1. Extracting the PGconn connection from the libpq_source structure
2. Executing the PostgreSQL function  using 
3. Parsing the returned LSN string (in format 'XXXXXXXX/XXXXXXXX') into high and low 32-bit components
4. Reconstructing the LSN as a 64-bit XLogRecPtr value
5. Cleaning up the allocated string

The LSN format is parsed using sscanf with the pattern '%X/%X' to extract the hexadecimal high and low portions, which are then combined into a single 64-bit value.

## Parameters
- : Generic rewind_source pointer that will be cast to libpq_source to access the connection

## Dependencies
- Functions called/Symbols referenced:
  - [run_simple_query](../r/run_simple_query.md)
  - [pg_free](../p/pg_free.md)
  - [pg_fatal](../p/pg_fatal.md)
  - sscanf (standard library)
- Types referenced:
  - [rewind_source](../r/rewind_source.md)
  - libpq_source
  - XLogRecPtr
  - [PGconn](../P/PGconn.md)
- Called from:
  - Used as function pointer in init_libpq_source (set at line 95)

## Notes and Other Information
- This is a static function, only accessible within the libpq_source.c file
- The function implements the get_current_wal_insert_lsn method of the rewind_source interface
- LSN parsing assumes the standard PostgreSQL LSN format 'XXXXXXXX/XXXXXXXX' (8 hex digits, slash, 8 hex digits)
- The function will terminate the program if the LSN format is not recognized
- This LSN value is critical for pg_rewind to determine the timeline divergence point between source and target servers
- The returned XLogRecPtr is a 64-bit value representing the byte position in the WAL stream

## Simplified Source

```c
static XLogRecPtr
libpq_get_current_wal_insert_lsn(rewind_source *source)
{
    PGconn *conn = ((libpq_source *) source)->conn;
    XLogRecPtr result;
    uint32 hi, lo;
    char *val;

    // Query current WAL insert position
    val = run_simple_query(conn, "SELECT pg_current_wal_insert_lsn()");

    // Parse LSN format "XXXXXXXX/XXXXXXXX"
    if (sscanf(val, "%X/%X", &hi, &lo) != 2)
        pg_fatal("unrecognized result \"%s\" for current WAL insert location", val);

    // Combine high and low parts into 64-bit LSN
    result = ((uint64) hi) << 32 | lo;

    pg_free(val);
    return result;
}
```