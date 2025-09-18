# warn_or_exit_horribly

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1874 - 1925

## Overview
A comprehensive error reporting function that provides contextual error messages and either exits the program or increments the error counter based on configuration.

## Definition


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
  - pg_log_generic_v
  - PG_LOG_ERROR, PG_LOG_PRIMARY
  - exit_nicely
- Called from (representative examples):
  - dump_lo_buf
  - _selectOutputSchema
  - _selectTablespace
  - _selectTableAccessMethod
  - ExecuteSqlCommand
  - EndDBCopyMode

## Notes and Other Information
- Tracks error state to avoid duplicate contextual messages for the same stage/TOC entry
- Provides detailed TOC entry information including dump ID, catalog IDs, description, tag, and owner
- Behavior depends on the exit_on_error flag: either terminates with exit_nicely(1) or increments n_errors counter
- Central error handling point that ensures consistent error reporting across the entire pg_dump system