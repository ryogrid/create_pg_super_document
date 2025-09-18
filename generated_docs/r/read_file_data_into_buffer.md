# read_file_data_into_buffer

## Location
[src/backend/backup/basebackup.c:1847-1949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L1847-L1949)

## Overview
read_file_data_into_buffer reads data from a file into the backup sink's buffer with optional checksum verification and retry logic for handling concurrent database modifications.

## Definition
```c
static off_t read_file_data_into_buffer(bbsink *sink, const char *readfilename, int fd,
                                       off_t offset, size_t length, BlockNumber blkno,
                                       bool verify_checksum, int *checksum_failures)
```

## Detailed Description
This function serves as the low-level file reading component for PostgreSQL's base backup system. It reads data from a file at a specified offset and length into the backup sink's buffer, with sophisticated handling for checksum verification. The function includes retry logic to handle "torn page" scenarios where concurrent writes might result in inconsistent page states during backup. When checksum verification fails on the first attempt, it retries the read once to allow concurrent write operations to complete, then reports any persistent checksum failures.

The function is designed to handle concurrent database modifications gracefully by detecting file truncations and adjusting the read amount accordingly. It limits checksum failure warnings to avoid flooding logs during problematic backup operations.

## Parameters / Member Variables
- `sink`: bbsink object containing the buffer to read data into
- `readfilename`: Name of the file being read (used for error reporting)
- `fd`: File descriptor of the open file to read from
- `offset`: File offset from which to begin reading
- `length`: Maximum amount of data to read (limited by buffer size)
- `blkno`: Block number of the first page relative to relation start (for checksum verification)
- `verify_checksum`: Boolean flag indicating whether to perform checksum verification
- `checksum_failures`: Pointer to counter for tracking checksum failures

## Dependencies
- Functions called/Symbols referenced:
  - [basebackup_read_file](../b/basebackup_read_file.md)
  - [verify_page_checksum](../v/verify_page_checksum.md)
  - PageHeader (for checksum access)
- Called from (representative examples):
  - [sendFile](../s/sendFile.md)

## Notes and Other Information
- Returns the actual number of bytes read, which may be less than requested if buffer is too small or file was truncated
- Only verifies checksums when data length is a multiple of BLCKSZ (block size)
- Implements retry logic for checksum failures to handle concurrent "torn page" writes
- Limits checksum failure warnings to first 5 failures per file to avoid log flooding
- Handles concurrent file truncation by returning partial data and letting caller handle the situation
- Designed to work with PostgreSQL's buffer management and page checksum system
- Located in src/backend/backup/basebackup.c:1847-1949