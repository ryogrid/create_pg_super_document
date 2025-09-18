# ShutdownWorkersHard

## Location
[src/bin/pg_dump/parallel.c:395-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L395-L445)

## Overview
Forcibly shuts down any remaining workers in a parallel dump operation, waiting for them to terminate. This function is called during error situations to ensure proper cleanup of worker processes.

## Definition


## Detailed Description
ShutdownWorkersHard is a cleanup function used in emergency situations during pg_dump parallel operations. Unlike normal shutdown procedures, this function is specifically designed to handle forceful termination when the main process encounters a fatal error (pg_fatal() situations). The function performs a three-step shutdown process:

1. **Signal closure**: Closes the write end of communication sockets to notify workers they should exit
2. **Force termination**: Sends termination signals to worker processes (platform-specific implementation)
3. **Wait for cleanup**: Waits for all workers to properly terminate

The implementation differs between Unix-like systems and Windows:
- On Unix systems: Uses SIGTERM signals sent to worker PIDs
- On Windows: Uses PostgreSQL's query cancellation mechanism with critical section protection

## Parameters / Member Variables
- : Pointer to ParallelState structure containing information about all worker processes, including their PIDs, communication pipes, and archive handles

## Dependencies
- Functions called/Symbols referenced:
  - closesocket (closes communication pipes)
  - kill (Unix: sends SIGTERM to worker processes)
  - [PQcancel](../P/PQcancel.md) (Windows: cancels active queries)
  - [WaitForTerminatingWorkers](../W/WaitForTerminatingWorkers.md) (waits for workers to finish termination)
  - EnterCriticalSection/LeaveCriticalSection (Windows: thread synchronization)
- Called from (representative examples):
  - [write_stderr](../w/write_stderr.md) (error handling context)
  - [archive_close_connection](../a/archive_close_connection.md) (connection cleanup context)

## Notes and Other Information
- This is a static function, only used within the parallel.c module
- Function is designed for error recovery scenarios, not normal operation
- Platform-specific implementations handle the differences between Unix process signals and Windows query cancellation
- The function includes error tolerance - it ignores errors when closing sockets since some workers might not have been fully initialized
- Critical section usage on Windows prevents race conditions during worker state changes
- Located in src/bin/pg_dump/parallel.c:395-445