# archive_close_connection

## Location
[src/bin/pg_dump/parallel.c:339-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L339-L394)

## Overview
An exit handler function that performs clean shutdown of database connections and worker processes in both parallel and non-parallel dump operations.

## Definition

```c
static void
archive_close_connection(int code, void *arg)
```
## Detailed Description
This static function serves as a cleanup handler registered with on_exit_nicely() to ensure proper resource cleanup when pg_dump or pg_restore processes terminate. The function handles three distinct operational contexts:

**Leader Process in Parallel Mode:**
- Forcibly shuts down all worker processes using ShutdownWorkersHard()
- Closes the leader's own database connection if it exists
- Takes responsibility for coordinating the entire parallel operation shutdown

**Worker Process/Thread in Parallel Mode:**
- Closes the worker's own database connection if active
- On Windows platforms, explicitly closes communication sockets (pipeRevRead and pipeRevWrite) to ensure proper EOF signaling to the leader process
- The socket closure is critical on Windows because threads don't generate EOF conditions automatically like separate processes do on Unix

**Non-Parallel Operation:**
- Simply closes the single database connection if it exists
- No worker coordination required since there's only one process

The function uses GetMyPSlot() to determine whether the calling context is a leader or worker, enabling appropriate cleanup behavior. This design ensures robust cleanup regardless of how the process terminates (normal exit, signal, error, etc.).

## Parameters / Member Variables
- `code`: Exit code indicating how the process is terminating (standard exit handler parameter)
- `*arg`: Void pointer to ShutdownInformation structure containing cleanup state information
## Dependencies
- Functions called/Symbols referenced:
  - [GetMyPSlot](../G/GetMyPSlot.md) (finds current worker's parallel slot)
  - [ShutdownWorkersHard](../S/ShutdownWorkersHard.md) (forcibly terminates all worker processes)
  - [DisconnectDatabase](../D/DisconnectDatabase.md) (closes database connections)
  - closesocket (Windows API for closing sockets)
  - [ShutdownInformation](../S/ShutdownInformation.md) (struct type for cleanup state)
  - [ParallelSlot](../P/ParallelSlot.md) (struct type for worker information)

- Called from (representative examples):
  - [on_exit_close_archive](../o/on_exit_close_archive.md) (registered as exit handler)
  - [write_stderr](../w/write_stderr.md) (context for error handling)

## Notes and Other Information
- This is a static function, accessible only within parallel.c
- Registered as an exit handler via on_exit_nicely() during process initialization
- Handles platform differences between Windows threading and Unix process models
- Critical for preventing resource leaks in parallel dump operations
- The Windows socket closure is essential for proper leader-worker communication termination
- Provides graceful cleanup even during abnormal process termination
- Works in conjunction with the broader PostgreSQL error handling and cleanup framework