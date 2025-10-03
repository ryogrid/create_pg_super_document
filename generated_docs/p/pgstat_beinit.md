# pgstat_beinit

## Location
[src/backend/utils/activity/backend_status.c:247-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/backend_status.c#L247-L272)

## Overview
Initializes the backend statistics state for a process and sets up the cleanup hook for process exit.

## Definition
```c
void pgstat_beinit(void)
```

## Detailed Description
This function performs the initial setup of backend statistics tracking for a process. It validates that MyProcNumber has been set to a valid value, assigns the process's entry in the shared BackendStatusArray, and registers a shutdown hook to ensure proper cleanup when the process exits. This function must be called after MyProcNumber is set but before any transactions begin, as the exit hook needs to run after the last transaction.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for MyProcNumber validation)
  - INVALID_PROC_NUMBER
  - NumBackendStatSlots
  - BackendStatusArray
  - MyBEEntry (global variable set)
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [pgstat_beshutdown_hook](pgstat_beshutdown_hook.md)
- Called from:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - [InitPostgres](../I/InitPostgres.md)

## Notes and Other Information
The function includes important assertions to ensure MyProcNumber is valid and within the expected range. It's crucial that MyDatabaseId may not be set yet when this function is called, which is why the shutdown hook must be careful about database-specific cleanup. This function is called from both regular backend initialization (InitPostgres) and auxiliary process initialization.

## Simplified Source

```c
// Simplified version of pgstat_beinit
void pgstat_beinit(void) {
    // Step 1: Validate that the process number has been assigned
    // MyProcNumber must be set to a valid index before calling this function
    Assert(MyProcNumber != INVALID_PROC_NUMBER);
    Assert(MyProcNumber >= 0 && MyProcNumber < NumBackendStatSlots);

    // Step 2: Connect this process to its statistics entry in shared memory
    // Each backend gets a dedicated slot in the shared BackendStatusArray
    MyBEEntry = &BackendStatusArray[MyProcNumber];

    // Step 3: Register cleanup function to run when process exits
    // This ensures statistics are properly cleaned up on process termination
    on_shmem_exit(pgstat_beshutdown_hook, 0);
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- Clarified the purpose of assertions and variable assignments
- Explained the relationship between MyProcNumber and BackendStatusArray
- Made the exit hook registration purpose more explicit