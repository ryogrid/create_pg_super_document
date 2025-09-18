# FindStreamingStart

## Location
src/bin/pg_basebackup/pg_receivewal.c: 268 - 369

## Overview
Determines the starting location for WAL streaming by analyzing existing WAL segment files in the destination directory to find the appropriate resume point.

## Definition


## Detailed Description
This function scans the destination directory for existing WAL (Write-Ahead Logging) files to determine where streaming should resume. It examines all WAL segment files, validates their completeness by checking file sizes, and identifies the highest timeline ID with the latest complete segment. The function supports multiple compression algorithms (none, gzip, LZ4) and handles the complexity of verifying compressed segment integrity.

The function implements sophisticated logic to handle different compression formats:
- For uncompressed segments: directly checks file size against expected WAL segment size
- For gzip-compressed segments: reads the last 4 bytes which contain uncompressed size information
- For LZ4-compressed segments: decompresses content to verify actual uncompressed size

If no valid WAL files are found in the directory, it returns InvalidXLogRecPtr to indicate streaming should start from the beginning.

## Parameters / Member Variables
- : Output parameter that receives the timeline ID of the highest timeline found with complete segments

## Dependencies
- Functions called/Symbols referenced:
  - get_destination_dir
  - readdir
  - is_xlogfilename
  - XLogFromFileName
  - stat, open, lseek, read, close (system calls)
  - pg_log_warning
  - pg_fatal
- Called from (representative examples):
  - StreamLog (in pg_receivewal.c:537)

## Notes and Other Information
- This is a static function, only accessible within pg_receivewal.c
- Handles three compression algorithms: none, gzip, and LZ4 (if compiled with LZ4 support)
- Returns InvalidXLogRecPtr when no suitable WAL files are found
- Performs comprehensive validation of WAL segment file integrity
- Uses timeline ID to handle PostgreSQL's timeline switching mechanism
- Critical for ensuring WAL streaming continuity and avoiding gaps or overlaps
- The function's logic accounts for partial files (*.partial) which are skipped during size validation