# warn_or_exit_horribly

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1874-1925](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1874-L1925)

## Overview
A comprehensive error reporting function that provides contextual error messages and either exits the program or increments the error counter based on configuration.

## Definition

```c
void
warn_or_exit_horribly(ArchiveHandle *AH, const char *fmt,...)
```
## Detailed Description
The  function serves as the central error handling mechanism for the PostgreSQL archiver. It provides rich contextual information about where errors occur, including the current processing stage (INITIALIZING, PROCESSING, FINALIZING) and details about the current TOC entry being processed. The function can either exit the program immediately or increment an error counter for later handling, depending on the  setting in the archive handle.

## Parameters / Member Variables
- : Archive handle containing error tracking state and configuration
- : Printf-style format string for the error message
- : Variable arguments for the format string

## Dependencies
- Functions called/Symbols referenced:
  - STAGE_NONE, STAGE_INITIALIZING, STAGE_PROCESSING, STAGE_FINALIZING
  - pg_log_info
  - [pg_log_generic_v](../p/pg_log_generic_v.md)
  - PG_LOG_ERROR, PG_LOG_PRIMARY
  - [exit_nicely](../e/exit_nicely.md)
- Called from (representative examples):
  - [dump_lo_buf](../d/dump_lo_buf.md)
  - [_selectOutputSchema](../s/_selectOutputSchema.md)
  - [_selectTablespace](../s/_selectTablespace.md)
  - [_selectTableAccessMethod](../s/_selectTableAccessMethod.md)
  - [ExecuteSqlCommand](../E/ExecuteSqlCommand.md)
  - [EndDBCopyMode](../E/EndDBCopyMode.md)

## Notes and Other Information
- Tracks error state to avoid duplicate contextual messages for the same stage/TOC entry
- Provides detailed TOC entry information including dump ID, catalog IDs, description, tag, and owner
- Behavior depends on the exit_on_error flag: either terminates with exit_nicely(1) or increments n_errors counter
- Central error handling point that ensures consistent error reporting across the entire pg_dump system