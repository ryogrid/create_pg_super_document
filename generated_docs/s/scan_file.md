# scan_file

## Location
src/bin/pg_checksums/pg_checksums.c: 176 - 299

## Overview
The `scan_file` function processes individual PostgreSQL data files to either verify existing page checksums or enable checksums by calculating and writing them to each page header.

## Definition
```c
static void scan_file(const char *fn, int segmentno)
```

## Detailed Description
This function is the core processing routine in the pg_checksums utility that handles individual PostgreSQL data files. It operates in two distinct modes:

1. **CHECK mode (PG_MODE_CHECK)**: Verifies existing checksums in data pages
   - Reads each page and calculates its expected checksum
   - Compares the calculated checksum with the stored checksum in the page header
   - Reports mismatches and increments the bad block counter
   - Opens files in read-only mode

2. **ENABLE mode (PG_MODE_ENABLE)**: Enables checksums by writing them to page headers
   - Calculates checksums for each page
   - Writes the checksum back to the page header if different from existing value
   - Opens files in read-write mode
   - Tracks the number of blocks and files that were actually modified

The function processes files block by block (8KB pages), handling I/O errors gracefully and providing detailed error messages. It skips new pages (identified by `PageIsNew`) since they dont require checksum processing yet. Progress reporting is integrated to provide real-time feedback during long-running operations.

Key operational aspects:
- Uses aligned I/O buffers for optimal performance
- Handles partial reads and writes with detailed error reporting
- Updates global counters for progress tracking and statistics
- Calculates block numbers considering segment boundaries for large relations

## Parameters / Member Variables
- `fn`: The full file path of the PostgreSQL data file to process
- `segmentno`: The segment number of this file within a relation (for large relations split across multiple files)

## Dependencies
- Functions called/Symbols referenced:
  - `open`, `close`, `read`, `write`, `lseek` (standard POSIX I/O functions)
  - [pg_checksum_page](../p/pg_checksum_page.md) (PostgreSQL checksum calculation function)
  - [PageIsNew](../P/PageIsNew.md) (PostgreSQL macro to check if page is newly allocated)
  - [pg_fatal](../p/pg_fatal.md), `pg_log_error`, `pg_log_info` (PostgreSQL logging functions)
  - [progress_report](../p/progress_report.md) (local progress reporting function)
  - `PGIOAlignedBlock`, `PageHeader` (PostgreSQL data structure types)
  - Global variables: `mode`, `files_scanned`, `blocks_scanned`, `current_size`, `badblocks`, `files_written`, `blocks_written`, `showprogress`, `verbose`, `ControlFile`

- Called from (representative examples):
  - [scan_directory](scan_directory.md) function in pg_checksums.c during directory traversal

## Notes and Other Information
- This is a static function with internal linkage, accessible only within pg_checksums.c
- Uses `PGIOAlignedBlock` for proper memory alignment required for direct I/O operations
- The function calculates the global block number by adding `blockno + segmentno * RELSEG_SIZE` to handle PostgreSQLs file segmentation scheme
- Error handling includes both system-level I/O errors and PostgreSQL-specific checksum verification failures
- In ENABLE mode, the function optimizes by only writing blocks whose checksums have actually changed
- Progress reporting is called after processing each block to provide responsive user feedback
- The function maintains several global counters that are used for final reporting and progress tracking
- File operations use `PG_BINARY` flag for proper binary file handling across platforms
- The checksum verification only proceeds if the data checksum version matches the expected version (`PG_DATA_CHECKSUM_VERSION`)