# remove_temp

## Location
src/test/regress/pg_regress.c: 467 - 478

## Overview
Removes the temporary socket directory and its contents created during PostgreSQL regression testing, handling cleanup when the postmaster exit timing is indeterminate.

## Definition


## Detailed Description
This function performs cleanup of the temporary Unix socket directory used during pg_regress operations. Since pg_regress never waits for postmaster exit, it cannot rely on the postmaster to clean up its socket and lock files. The function proactively removes these files and the containing directory to ensure clean test environment teardown. It's designed to be safe for execution from signal handlers and ignores errors since temporary directory leaks are considered non-critical failures.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call)
  - rmdir (system call)
  - Assert (macro)
  - temp_sockdir (global variable)
  - sockself (global variable)
  - socklock (global variable)
- Called from (representative examples):
  - signal_remove_temp
  - make_temp_sockdir

## Notes and Other Information
- Function is marked static (internal to pg_regress.c)
- Can safely run from signal handlers on Unix systems
- Code is not acceptable for Windows signal handlers (similar to initdb.c:trapsig())
- On Windows, pg_regress typically doesn't use Unix sockets by default
- Errors are intentionally ignored since temporary directory leaks are considered unimportant
- Uses Assert to verify temp_sockdir is not NULL before proceeding
- Located in src/test/regress/pg_regress.c:467-478