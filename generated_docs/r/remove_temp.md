# remove_temp

## Location
[src/test/regress/pg_regress.c:467-478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L467-L478)

## Overview
Removes the temporary socket directory and its contents created during PostgreSQL regression testing, handling cleanup when the postmaster exit timing is indeterminate.

## Definition

```c
static void
remove_temp(void)
```
## Detailed Description
This function performs cleanup of the temporary Unix socket directory used during pg_regress operations. Since pg_regress never waits for postmaster exit, it cannot rely on the postmaster to clean up its socket and lock files. The function proactively removes these files and the containing directory to ensure clean test environment teardown. It's designed to be safe for execution from signal handlers and ignores errors since temporary directory leaks are considered non-critical failures.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call)
  - rmdir (system call)
  - Assert (macro)
  - temp_sockdir (global variable)
  - sockself (global variable)
  - socklock (global variable)
- Called from (representative examples):
  - [signal_remove_temp](../s/signal_remove_temp.md)
  - [make_temp_sockdir](../m/make_temp_sockdir.md)

## Notes and Other Information
- Function is marked static (internal to pg_regress.c)
- Can safely run from signal handlers on Unix systems
- Code is not acceptable for Windows signal handlers (similar to initdb.c:trapsig())
- On Windows, pg_regress typically doesn't use Unix sockets by default
- Errors are intentionally ignored since temporary directory leaks are considered unimportant
- Uses Assert to verify temp_sockdir is not NULL before proceeding
- Located in src/test/regress/pg_regress.c:467-478

## Simplified Source

```c
static void remove_temp(void) {
    Assert(temp_sockdir);

    // Clean up socket files and directory
    // Errors ignored - temp directory leaks are non-critical
    unlink(sockself);   // Remove socket file
    unlink(socklock);   // Remove lock file
    rmdir(temp_sockdir); // Remove directory
}
```