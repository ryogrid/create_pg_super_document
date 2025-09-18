# copy_file

## Location
[src/bin/pg_combinebackup/copy_file.c:49-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/copy_file.c#L49-L126)

## Overview
Copies a file from source to destination using a buffered read-write operation with periodic flushing for performance optimization.

## Definition


## Detailed Description
The  function performs a complete file copy operation using PostgreSQL's transient file management system. It reads data in 8-block chunks (COPY_BUF_SIZE = 8 * BLCKSZ) and periodically flushes the destination file to avoid overwhelming the system cache. The flush frequency is platform-dependent: every 1MB on most systems, but every 32MB on macOS due to APFS performance characteristics. The function includes comprehensive error handling and uses PostgreSQL's wait event reporting for monitoring I/O operations.

## Parameters / Member Variables
- : Source file path to copy from
- : Destination file path to copy to

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation
  -  - PostgreSQL file opening
  -  - PostgreSQL file closing  
  -  - Data flushing
  -  - Wait event reporting
  -  - Signal handling
  -  - Memory deallocation
- Called from (representative examples):
  -  (src/backend/storage/file/copydir.c:74)
  -  (src/backend/storage/file/reinit.c:315)
  - Various COPY command functions in copyfrom.c and copyto.c

## Notes and Other Information
- Uses platform-specific flush distances for optimal performance
- Implements interrupt checking for cancellation support
- Part of PostgreSQL's core storage file management infrastructure
- Location: src/backend/storage/file/copydir.c:117-216