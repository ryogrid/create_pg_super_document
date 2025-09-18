# do_lo_import

## Location
src/bin/psql/large_obj.c: 176 - 238

## Overview
Imports a file from the filesystem into the PostgreSQL database as a large object, with optional comment support.

## Definition
```c
bool do_lo_import(const char *filename_arg, const char *comment_arg)
```

## Detailed Description
The `do_lo_import` function implements the PostgreSQL \lo_import command functionality in psql. It creates a new large object in the database by copying the contents of a specified file from the filesystem. The function manages transaction boundaries automatically and provides error handling throughout the import process. If a comment is provided, it creates a COMMENT ON LARGE OBJECT statement to associate descriptive text with the imported object. Upon successful completion, it sets the LASTOID variable to the OID of the newly created large object.

## Parameters / Member Variables
- `filename_arg`: Path to the file on the filesystem to be imported as a large object
- `comment_arg`: Optional comment string to be associated with the large object (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - start_lo_xact (transaction management)
  - SetCancelConn/ResetCancelConn (cancellation handling)
  - lo_import (libpq large object import function)
  - pg_log_info (error logging)
  - fail_lo_xact (transaction rollback)
  - pg_malloc_extended (memory allocation)
  - PQescapeStringConn (SQL string escaping)
  - PSQLexec (SQL execution)
  - finish_lo_xact (transaction commit)
  - print_lo_result (result output)
  - SetVariable (psql variable setting)
- Called from (representative examples):
  - exec_command_lo (psql command execution)

## Notes and Other Information
- Returns true on success, false on failure
- Automatically manages transaction boundaries with start_lo_xact/finish_lo_xact
- Sets the LASTOID psql variable to the OID of the imported large object
- Provides proper error handling and cleanup on failure
- Uses proper SQL string escaping when adding comments to prevent injection
- Part of psql's large object management subsystem