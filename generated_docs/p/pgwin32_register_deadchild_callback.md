# pgwin32_register_deadchild_callback

## Location
[src/backend/postmaster/postmaster.c:4677-4704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4677-L4704)

## Overview
Registers a Windows-specific callback to be invoked when a child process terminates, using the Windows thread pool to handle the wait operation asynchronously.

## Definition
```c
void pgwin32_register_deadchild_callback(HANDLE procHandle, DWORD procId)
```

## Detailed Description
This function is part of PostgreSQL's Windows-specific process management infrastructure. It registers a child process for asynchronous death notification using the Windows RegisterWaitForSingleObject API. When the child process terminates, the registered callback (`pgwin32_deadchild_callback`) will be executed in a Windows thread pool thread, which then posts a completion notification that can be handled by the main PostgreSQL process through `waitpid()`.

The function allocates a `win32_deadchild_waitinfo` structure to store the process handle, process ID, and wait handle for later cleanup. The wait operation is configured to execute only once (`WT_EXECUTEONLYONCE`) and to run in a wait thread (`WT_EXECUTEINWAITTHREAD`) with infinite timeout.

## Parameters / Member Variables
- `procHandle`: Windows process handle of the child process to monitor
- `procId`: Windows process ID (DWORD) of the child process

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](palloc.md) - PostgreSQL memory allocation
  - `RegisterWaitForSingleObject` - Windows API for asynchronous wait
  - [pgwin32_deadchild_callback](pgwin32_deadchild_callback.md) - Callback function executed when child dies
  - `ereport` - PostgreSQL error reporting
  - `GetLastError` - Windows API for error code retrieval

- Called from (representative examples):
  - `launch_backend.c:560` - When launching new backend processes

## Notes and Other Information
- This function is Windows-specific and only compiled on WIN32 platforms
- The callback mechanism ensures that child process termination is handled asynchronously without blocking the main postmaster process
- Memory allocated for the `win32_deadchild_waitinfo` structure is freed later by the `waitpid()` implementation
- If `RegisterWaitForSingleObject` fails, the function reports a FATAL error, as this indicates a critical system failure
- The callback runs in a separate Windows thread pool thread, requiring all operations within the callback to be thread-safe