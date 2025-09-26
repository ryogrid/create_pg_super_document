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