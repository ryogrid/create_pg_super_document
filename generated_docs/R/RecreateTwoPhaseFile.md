# RecreateTwoPhaseFile

## Location
[src/backend/access/transam/twophase.c:1727-1806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1727-L1806)

## Overview
RecreateTwoPhaseFile reconstructs a two-phase commit state file on disk from in-memory content, computing and appending a CRC checksum for data integrity.

## Definition
static void RecreateTwoPhaseFile(TransactionId xid, void *content, int len)

## Detailed Description
This static function is responsible for recreating two-phase commit state files from memory during WAL replay and checkpoint operations. It performs a complete file recreation process including CRC computation, atomic file creation, and proper synchronization. The function first computes a CRC32C checksum for the provided content to ensure data integrity, then creates a new file (or truncates an existing one) and writes both the content and the computed CRC. Critical for durability, it explicitly performs fsync operations to ensure the data reaches persistent storage, since during replay scenarios there may not be shared memory structures to trigger normal checkpoint fsync behavior. The function uses PostgreSQL's transient file management system and includes comprehensive error handling with proper wait event reporting for monitoring purposes.

## Parameters / Member Variables
- `xid`: The transaction ID for which to create the two-phase commit state file
- `content`: Pointer to the memory buffer containing the two-phase commit state data to write (excludes CRC)
- `len`: Length in bytes of the content data (does not include the CRC that will be appended)

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseFilePath](../T/TwoPhaseFilePath.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - write
  - [pg_fsync](../p/pg_fsync.md)
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - INIT_CRC32C/COMP_CRC32C/FIN_CRC32C (CRC computation macros)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end
- Called from (representative examples):
  - [CheckPointTwoPhase](../C/CheckPointTwoPhase.md)

## Notes and Other Information
- This is a static function, only accessible within the twophase.c module
- Explicitly performs fsync to ensure durability, particularly important during WAL replay when normal checkpoint mechanisms may not apply
- Uses O_CREAT | O_TRUNC flags to ensure clean file creation, overwriting any existing content
- Includes comprehensive error reporting for all file operations (open, write, fsync, close)
- Integrates with PostgreSQL's wait event reporting system for monitoring I/O operations
- CRC is computed using the CRC32C algorithm and written as a separate trailing section after the main content

## Simplified Source

```c
// Simplified version of RecreateTwoPhaseFile
static void RecreateTwoPhaseFile(TransactionId xid, void *content, int len)
{
    char path[MAXPGPATH];
    pg_crc32c statefile_crc;
    int fd;

    // Step 1: Compute CRC checksum for data integrity
    INIT_CRC32C(statefile_crc);
    COMP_CRC32C(statefile_crc, content, len);
    FIN_CRC32C(statefile_crc);

    // Step 2: Generate file path and open for writing
    TwoPhaseFilePath(path, xid);
    fd = OpenTransientFile(path, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
    if (fd < 0)
        ereport(ERROR, "could not recreate file");

    // Step 3: Write content data to file
    pgstat_report_wait_start(WAIT_EVENT_TWOPHASE_FILE_WRITE);
    if (write(fd, content, len) != len) {
        ereport(ERROR, "could not write content to file");
    }

    // Step 4: Write CRC checksum to file
    if (write(fd, &statefile_crc, sizeof(pg_crc32c)) != sizeof(pg_crc32c)) {
        ereport(ERROR, "could not write CRC to file");
    }
    pgstat_report_wait_end();

    // Step 5: Force sync to disk for durability
    pgstat_report_wait_start(WAIT_EVENT_TWOPHASE_FILE_SYNC);
    if (pg_fsync(fd) != 0)
        ereport(ERROR, "could not fsync file");
    pgstat_report_wait_end();

    // Step 6: Close the file
    if (CloseTransientFile(fd) != 0)
        ereport(ERROR, "could not close file");
}
```

Key simplifications made:
- Removed detailed error handling code (errno checks, specific error messages)
- Simplified error reporting to show essential error cases only
- Consolidated duplicate error handling patterns
- Added step-by-step comments to clarify the algorithm flow
- Abstracted low-level file operation details while preserving core logic
- Maintained all essential operations: CRC computation, file creation, content writing, sync, and cleanup