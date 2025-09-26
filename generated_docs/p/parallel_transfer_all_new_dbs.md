# parallel_transfer_all_new_dbs

## Location
[src/bin/pg_upgrade/parallel.c:172-262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/parallel.c#L172-L262)

## Overview
Performs parallel transfer of all new databases during PostgreSQL upgrade, enabling concurrent tablespace transfers for improved performance.

## Definition

```c
struct */
		new_arg->old_db_arr = old_db_arr;
```
## Detailed Description
This function provides parallel execution capabilities for transferring all new databases during a PostgreSQL upgrade. It has the same API as transfer_all_new_dbs but adds parallel execution by transferring multiple tablespaces concurrently. The function manages worker processes (Unix) or threads (Windows) to handle database transfers in parallel, significantly improving upgrade performance for systems with multiple tablespaces.

When parallel jobs are disabled (user_opts.jobs <= 1), it falls back to sequential execution using transfer_all_new_dbs. In parallel mode, it handles the complete lifecycle of worker processes/threads, including proper job scheduling, resource management, and error handling.

The function ensures proper stdio state before forking and includes comprehensive error handling for process/thread creation failures.

## Parameters / Member Variables
- : Array of database information structures from the old cluster
- : Array of database information structures for the new cluster  
- : Path to the old PostgreSQL data directory
- : Path to the new PostgreSQL data directory
- : Path to the specific old tablespace being transferred (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [transfer_all_new_dbs](../t/transfer_all_new_dbs.md)
  - [reap_child](../r/reap_child.md)
  - [win32_transfer_all_new_dbs](../w/win32_transfer_all_new_dbs.md) (Windows only)
  - [pg_malloc](pg_malloc.md)
  - [pg_malloc0](pg_malloc0.md)
  - [pg_free](pg_free.md)
  - [pg_strdup](pg_strdup.md)
  - fork (Unix)
  - _beginthreadex (Windows)
- Called from (representative examples):
  - [transfer_all_new_tablespaces](../t/transfer_all_new_tablespaces.md)

## Notes and Other Information
- Platform-specific implementation: Uses fork() on Unix systems and _beginthreadex() on Windows
- Thread safety: On Windows, maintains thread-safe argument structures with proper memory management
- Memory management: Carefully manages allocation and deallocation of thread arguments on Windows
- Job control: Respects user_opts.jobs limit and implements job harvesting through reap_child()
- Performance optimization: Enables concurrent tablespace transfers, dramatically improving upgrade time for large databases
- Error handling: Uses pg_fatal() for critical errors that should terminate the upgrade process
- Process isolation: On Unix, uses _exit(0) to avoid atexit() functions in child processes