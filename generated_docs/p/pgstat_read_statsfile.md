# pgstat_read_statsfile

## Location
src/backend/utils/activity/pgstat.c: 1493 - 1693

## Overview
The  function reads an existing statistics file from disk and loads all statistics data into the shared memory hash table, restoring previously persisted statistics after a PostgreSQL restart.

## Definition


## Detailed Description
This function is responsible for reading the permanent statistics file and populating the shared statistics hash table during PostgreSQL startup or statistics system initialization. It reads both fixed statistics structures (like archiver, bgwriter, checkpointer, IO, SLRU, and WAL stats) and variable statistics entries (identified by hash keys or names).

The function implements a robust file format validation and error handling mechanism. If the statistics file doesn't exist (common on first startup or after stats collection was disabled), the function gracefully returns, allowing PostgreSQL to start with empty statistics. For corrupted files or other errors, it calls  to ensure a clean state.

The reading process follows a specific file format:
1. Format ID validation
2. Fixed statistics structures (archiver, bgwriter, checkpointer, IO, SLRU, WAL)
3. Variable entries marked with 'S' (normal entries) or 'N' (named entries like slots)
4. End marker 'E'

## Parameters / Member Variables
This function takes no parameters as it operates on global state and predefined file paths.

## Dependencies
- Functions called/Symbols referenced:
  - AllocateFile: Opens the statistics file for reading
  - read_chunk_s/read_chunk: Reads structured data from the file
  - pgstat_reset_after_failure: Called when file reading fails
  - dshash_find_or_insert: Inserts statistics entries into shared hash table
  - dshash_release_lock: Releases locks on hash table entries
  - pgstat_init_entry: Initializes a new statistics entry
  - pgstat_get_kind_info: Gets metadata for statistics entry types
  - pgstat_get_entry_len/pgstat_get_entry_data: Accesses entry size and data
  - FreeFile: Closes the file handle
  - unlink: Removes the statistics file after successful reading

- Called from (representative examples):
  - pgstat_restore_stats: Main function that orchestrates statistics restoration

## Notes and Other Information
- This function must only be called from a single process accessing shared stats (no locking required)
- Should not be called from the postmaster process
- The function removes the permanent statistics file after successful reading to prevent reprocessing
- Implements comprehensive error handling with detailed logging
- Supports both hash-key identified entries and name-identified entries (e.g., replication slots)
- File format validation prevents corruption from causing system instability
- Located in src/backend/utils/activity/pgstat.c:1493-1693