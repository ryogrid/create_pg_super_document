# pgarch_ArchiverCopyLoop

## Location
src/backend/postmaster/pgarch.c: 380 - 515

## Overview
The core archiving function that discovers WAL files ready for archival and processes them sequentially with retry logic and error handling.

## Definition
static void pgarch_ArchiverCopyLoop(void)

## Detailed Description
pgarch_ArchiverCopyLoop implements the main archival processing logic for the PostgreSQL archiver. It performs the following key operations:

1. **File Discovery**: Uses pgarch_readyXlog() to find WAL files marked with .ready status that need archiving
2. **Sequential Processing**: Archives files one at a time in a loop until all ready files are processed
3. **Retry Logic**: Implements retry mechanisms with exponential backoff for failed archive operations
4. **Orphan Cleanup**: Handles orphaned .ready status files for WAL files that no longer exist
5. **Configuration Validation**: Checks that archiving is properly configured before attempting operations
6. **Graceful Shutdown**: Respects shutdown signals and postmaster death during processing
7. **Statistics Reporting**: Reports successful and failed archive attempts to the statistics collector

The function includes robust error handling for various failure scenarios including missing WAL files, configuration errors, and repeated archive command failures. It uses a retry system with sleep intervals (1 second) and maximum retry limits (NUM_ARCHIVE_RETRIES and NUM_ORPHAN_CLEANUP_RETRIES).

## Parameters / Member Variables
- No parameters (void function)
- Local variables:
  - : Buffer for WAL filename
  - : Counter for archive operation failures
  - : Counter for orphan cleanup failures

## Dependencies
- Functions called/Symbols referenced:
  - [pgarch_readyXlog](pgarch_readyXlog.md) (find next WAL file ready for archival)
  - [PostmasterIsAlive](../P/PostmasterIsAlive.md) (check if postmaster process is running)
  - [HandlePgArchInterrupts](../H/HandlePgArchInterrupts.md) (process configuration updates and barriers)
  - StatusFilePath (construct status file paths)
  - [pgarch_archiveXlog](pgarch_archiveXlog.md) (perform actual WAL file archival)
  - [pgarch_archiveDone](pgarch_archiveDone.md) (mark file as successfully archived)
  - [pgstat_report_archiver](pgstat_report_archiver.md) (report archival statistics)
  - [pg_usleep](pg_usleep.md) (sleep between retries)
  - unlink (remove orphan status files)
- Constants used:
  - MAX_XFN_CHARS, NUM_ARCHIVE_RETRIES, NUM_ORPHAN_CLEANUP_RETRIES, XLOGDIR
- Called from (representative examples):
  - [pgarch_MainLoop](pgarch_MainLoop.md) (main archiver loop)

## Notes and Other Information
- This is a static function internal to the pgarch.c module
- Implements defensive programming with checks for shutdown conditions and postmaster death
- Uses callback-based architecture for archival operations through ArchiveCallbacks
- Handles edge cases like system crashes that leave orphaned status files
- Includes rate limiting through sleep intervals to avoid overwhelming the system during failures
- The function is designed to be interruptible and respects PostgreSQL's shutdown mechanisms