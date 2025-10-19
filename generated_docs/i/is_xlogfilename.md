# is_xlogfilename

## Location
[src/bin/pg_basebackup/pg_receivewal.c:116-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_receivewal.c#L116-L183)

## Overview
A utility function that validates whether a given filename matches the expected format of a PostgreSQL Write-Ahead Log (WAL) file and determines its compression and completion status.

## Definition

```c
static bool
is_xlogfilename(const char *filename, bool *ispartial,
				pg_compress_algorithm *wal_compression_algorithm)
```
## Detailed Description
The is_xlogfilename function performs comprehensive validation of WAL filenames by checking if they conform to PostgreSQL's WAL file naming conventions. It identifies whether a file is a complete or partial WAL segment and determines the compression algorithm used (none, gzip, or LZ4). The function first validates that the filename starts with exactly 24 hexadecimal characters (the standard WAL filename pattern), then checks various combinations of file extensions to determine the file's compression and completion status. This is crucial for WAL file management in streaming replication and backup scenarios.

## Parameters / Member Variables
- `*filename`: The filename string to validate against WAL file naming conventions
- `*ispartial`: Output parameter indicating whether the file is a partial WAL segment (not yet complete)
- `*wal_compression_algorithm`: Output parameter specifying the compression method used (none, gzip, or LZ4)
## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - strspn (standard C library function) 
  - strcmp (standard C library function)
  - XLOG_FNAME_LEN (constant defining WAL filename length)
  - PG_COMPRESSION_NONE (compression algorithm constant)
  - PG_COMPRESSION_GZIP (compression algorithm constant)
  - PG_COMPRESSION_LZ4 (compression algorithm constant)
  - [pg_compress_algorithm](../p/pg_compress_algorithm.md) (enum type)
- Called from (representative examples):
  - [FindStreamingStart](../F/FindStreamingStart.md) (in pg_receivewal.c)

## Notes and Other Information
- This is a static function with file-local scope within pg_receivewal.c
- Supports detection of both complete and partial WAL files (.partial extension)
- Handles multiple compression formats: uncompressed, gzip (.gz), and LZ4 (.lz4)
- WAL filenames must begin with exactly 24 hexadecimal characters to be considered valid
- The function uses output parameters to return multiple pieces of information about the file
- Critical for pg_receivewal utility's WAL file management and validation
- Returns false for any filename that doesn't match known WAL file patterns

## Simplified Source

```c
static bool is_xlogfilename(const char *filename, bool *ispartial,
                           pg_compress_algorithm *wal_compression_algorithm) {
    size_t fname_len = strlen(filename);
    size_t xlog_pattern_len = strspn(filename, "0123456789ABCDEF");

    // Must start with exactly 24 hex characters (standard WAL filename)
    if (xlog_pattern_len != XLOG_FNAME_LEN) {
        return false;
    }

    // Check various WAL file formats
    if (fname_len == XLOG_FNAME_LEN) {
        // Uncompressed complete WAL file
        *ispartial = false;
        *wal_compression_algorithm = PG_COMPRESSION_NONE;
        return true;
    } else if (fname_len == XLOG_FNAME_LEN + strlen(".gz") &&
               strcmp(filename + XLOG_FNAME_LEN, ".gz") == 0) {
        // Gzip compressed complete WAL file
        *ispartial = false;
        *wal_compression_algorithm = PG_COMPRESSION_GZIP;
        return true;
    } else if (fname_len == XLOG_FNAME_LEN + strlen(".lz4") &&
               strcmp(filename + XLOG_FNAME_LEN, ".lz4") == 0) {
        // LZ4 compressed complete WAL file
        *ispartial = false;
        *wal_compression_algorithm = PG_COMPRESSION_LZ4;
        return true;
    } else if (fname_len == XLOG_FNAME_LEN + strlen(".partial") &&
               strcmp(filename + XLOG_FNAME_LEN, ".partial") == 0) {
        // Uncompressed partial WAL file
        *ispartial = true;
        *wal_compression_algorithm = PG_COMPRESSION_NONE;
        return true;
    } else if (fname_len == XLOG_FNAME_LEN + strlen(".gz.partial") &&
               strcmp(filename + XLOG_FNAME_LEN, ".gz.partial") == 0) {
        // Gzip compressed partial WAL file
        *ispartial = true;
        *wal_compression_algorithm = PG_COMPRESSION_GZIP;
        return true;
    } else if (fname_len == XLOG_FNAME_LEN + strlen(".lz4.partial") &&
               strcmp(filename + XLOG_FNAME_LEN, ".lz4.partial") == 0) {
        // LZ4 compressed partial WAL file
        *ispartial = true;
        *wal_compression_algorithm = PG_COMPRESSION_LZ4;
        return true;
    }

    // Filename doesn't match any known WAL pattern
    return false;
}
```