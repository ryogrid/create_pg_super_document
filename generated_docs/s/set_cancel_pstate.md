# set_cancel_pstate

## Location
[src/bin/pg_dump/parallel.c:789-808](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/parallel.c#L789-L808)

## Overview
Sets the global parallel state pointer for signal handling in pg_dump's parallel backup system, providing thread-safe access to the ParallelState structure.

## Definition
static void set_cancel_pstate(ParallelState *pstate)

## Detailed Description
This function updates the global signal_info.pstate pointer to reference the specified ParallelState structure. It serves as a thread-safe mechanism to maintain a reference to the current parallel backup state that can be accessed by signal handlers for proper cleanup and cancellation operations.

The function implements platform-specific synchronization using critical sections on Windows to prevent race conditions between the main thread and the signal handling thread, while on Unix systems the pointer assignment is assumed to be atomic.

## Parameters / Member Variables
- pstate: Pointer to the ParallelState structure to be set as the current global parallel state, or NULL to clear the reference

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelState](../P/ParallelState.md) (type)
- Called from (representative examples):
  - [write_stderr](../w/write_stderr.md)
  - [ParallelBackupStart](../P/ParallelBackupStart.md)
  - [ParallelBackupEnd](../P/ParallelBackupEnd.md)

## Notes and Other Information
- Static function - only accessible within the parallel.c compilation unit
- Uses Windows critical sections for thread safety on that platform
- Essential for proper signal handling and cleanup in multi-threaded backup operations
- The pstate reference allows signal handlers to access worker thread information for cancellation

## Simplified Source

```c
static void
set_cancel_pstate(ParallelState *pstate)
{
    // Thread-safe update of global parallel state pointer
#ifdef WIN32
    EnterCriticalSection(&signal_info_lock);
#endif

    signal_info.pstate = pstate;

#ifdef WIN32
    LeaveCriticalSection(&signal_info_lock);
#endif
}
```