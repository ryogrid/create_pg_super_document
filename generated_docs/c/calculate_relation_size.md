# calculate_relation_size

## Location
src/backend/utils/adt/dbsize.c: 308 - 345

## Overview
A static utility function that calculates the total physical disk space consumed by a single fork of a PostgreSQL relation by summing up all its segment files.

## Definition
```c
static int64 calculate_relation_size(RelFileLocator *rfn, ProcNumber backend, ForkNumber forknum)
```

## Detailed Description
This function provides the core logic for determining the physical size of a relation fork on disk. PostgreSQL stores large relations across multiple segment files (numbered sequentially like relationname, relationname.1, relationname.2, etc.), and this function iterates through all segments to calculate the total size. 

The function constructs the file path for each segment using `relpathbackend`, then uses `stat()` system calls to obtain file sizes. It continues iterating through segments until it encounters a non-existent file (ENOENT), indicating the end of the segment chain. The function includes interrupt checking (`CHECK_FOR_INTERRUPTS`) to allow for cancellation during potentially long-running operations on large relations.

Notably, this function can safely be applied to temporary tables from other sessions without additional permission checks, as noted in the function comments.

## Parameters / Member Variables
- `rfn`: Pointer to RelFileLocator structure identifying the relation file
- `backend`: ProcNumber identifying the backend process (used for temp tables)
- `forknum`: Fork number specifying which fork of the relation to measure (main, FSM, VM, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - relpathbackend
  - snprintf
  - stat
  - CHECK_FOR_INTERRUPTS
  - ereport
  - errcode_for_file_access
  - errmsg
- Called from (representative examples):
  - pg_relation_size
  - calculate_toast_table_size
  - calculate_table_size
  - calculate_indexes_size

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit (dbsize.c)
- Handles multi-segment relations by iterating through numbered segment files
- Uses file system stat() calls to determine actual disk space usage
- Includes comprehensive error handling for file access issues
- Safe to use on temporary tables from other sessions
- The function is defined in src/backend/utils/adt/dbsize.c:308-345
- Segment numbering starts at 0 (base file has no suffix), then continues with .1, .2, etc.
- Critical component used by higher-level size calculation functions throughout PostgreSQL's size reporting system