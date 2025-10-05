# replorigin_reset

## Location
[src/backend/replication/logical/worker.c:4682-4690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4682-L4690)

## Overview
Resets the global replication origin session state variables to their invalid/initial values, typically used as an exit callback to clean up replication tracking state.

## Definition

```c
static void
replorigin_reset(int code, Datum arg)
```
## Detailed Description
This function serves as a cleanup routine for replication origin session tracking. It resets three critical global variables that track the current replication session state:

1. **Origin ID Reset**: Clears the current session's replication origin identifier
2. **LSN Reset**: Invalidates the current session's LSN (Log Sequence Number) tracking
3. **Timestamp Reset**: Clears the timestamp associated with the current replication session

The function is designed to be used as an exit callback, ensuring that replication origin state is properly cleaned up when a worker process terminates. This prevents stale replication tracking information from persisting across worker restarts or affecting other processes.

The function signature matches PostgreSQL's callback interface, accepting exit code and arbitrary data parameters, though neither parameter is actually used in the implementation.

## Parameters / Member Variables
- : Exit code parameter (unused in implementation) - follows PostgreSQL exit callback signature
- : Arbitrary data parameter (unused in implementation) - follows PostgreSQL exit callback signature

Global variables modified:
- : Set to InvalidRepOriginId to clear origin tracking
- : Set to InvalidXLogRecPtr to clear LSN tracking  
- : Set to 0 to clear timestamp tracking

## Dependencies
- Functions called/Symbols referenced:
  - InvalidRepOriginId: Constant representing an invalid replication origin ID
  - InvalidXLogRecPtr: Constant representing an invalid WAL LSN (implicitly used)
- Called from:
  - [start_apply](../s/start_apply.md): Registers this function as an exit callback
  - [SetupApplyOrSyncWorker](../S/SetupApplyOrSyncWorker.md): Sets up exit callback for worker cleanup

## Notes and Other Information
- This is a static function, internal to the worker.c file
- Designed specifically as an exit callback function matching PostgreSQL's callback signature conventions
- Critical for preventing replication state leakage between worker processes
- Simple but essential for proper resource cleanup in logical replication infrastructure
- The function parameters follow the standard PostgreSQL exit callback pattern but are not utilized in this specific implementation
- Ensures that global replication origin tracking variables return to a clean state upon worker termination

## Simplified Source

```c
static void
replorigin_reset(int code, Datum arg)
{
    // Reset replication origin session state to initial values
    replorigin_session_origin = InvalidRepOriginId;
    replorigin_session_origin_lsn = InvalidXLogRecPtr;
    replorigin_session_origin_timestamp = 0;
}
```