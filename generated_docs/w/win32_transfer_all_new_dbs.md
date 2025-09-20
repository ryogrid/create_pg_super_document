# win32_transfer_all_new_dbs

## Location
[src/bin/pg_upgrade/parallel.c:263-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/parallel.c#L263-L277)

## Overview
Windows-specific thread entry point function that transfers databases in parallel threads during PostgreSQL upgrade operations.

## Definition

```c
DWORD
win32_transfer_all_new_dbs(transfer_thread_arg *args)
```
## Detailed Description
This function serves as the thread entry point for Windows-based parallel database transfer operations in pg_upgrade. It's a thin wrapper around transfer_all_new_dbs that adapts the function signature to be compatible with Windows threading APIs (_beginthreadex). The function extracts database transfer parameters from the thread argument structure and executes the transfer using the standard transfer_all_new_dbs function.

Unlike win32_exec_prog, this function always returns 0 (success) as the transfer_all_new_dbs function handles its own error reporting and the thread termination itself indicates completion.

## Parameters / Member Variables
- : Pointer to transfer_thread_arg structure containing:
  - : Array of old database information structures
  - : Array of new database information structures  
  - : Path to old PostgreSQL data directory
  - : Path to new PostgreSQL data directory
  - : Path to old tablespace (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [transfer_all_new_dbs](../t/transfer_all_new_dbs.md)
  - transfer_thread_arg (struct type)
- Called from (representative examples):
  - [parallel_transfer_all_new_dbs](../p/parallel_transfer_all_new_dbs.md)
  - transfer_thread_arg

## Notes and Other Information
- Platform-specific: Only compiled and used on Windows platforms
- Threading: Designed as a thread entry point for _beginthreadex()
- Return value: Always returns 0 (DWORD) following Windows threading conventions
- Memory management: Does not manage the args structure - assumes it's managed by the calling thread
- Error handling: Delegates error handling to transfer_all_new_dbs function
- Thread lifecycle: Function return terminates the thread
- Performance: Enables concurrent database transfers on Windows systems