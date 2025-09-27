# ProcGlobalSemas

## Location
[src/backend/storage/lmgr/proc.c:122-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L122-L156)

## Overview
Reports the number of semaphores needed by InitProcGlobal for process synchronization.

## Definition
```c
int ProcGlobalSemas(void)
```

## Detailed Description
ProcGlobalSemas calculates the total number of semaphores required for PostgreSQL's process management system. Each process (backend or auxiliary) needs its own semaphore for synchronization purposes such as waiting for locks, coordinating with other processes, and managing process lifecycle events.

The function provides a simple calculation: one semaphore per backend process (including autovacuum workers) plus one semaphore for each auxiliary process (checkpointer, WAL writer, background writer, etc.).

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - MaxBackends (GUC parameter defining maximum number of backend processes)
  - NUM_AUXILIARY_PROCS (constant defining number of auxiliary processes)
- Called from:
  - [CalculateShmemSize](../C/CalculateShmemSize.md) (during shared memory and semaphore initialization)

## Notes and Other Information
- Essential for proper semaphore resource allocation during PostgreSQL startup
- Each semaphore enables process-level synchronization and coordination
- The count includes all types of backend processes: regular connections, autovacuum workers, and auxiliary processes
- Auxiliary processes include: checkpointer, background writer, WAL writer, WAL receiver, archiver, stats collector, etc.
- Failure to allocate sufficient semaphores will prevent PostgreSQL from starting properly

## Simplified Source

```c
// Simplified version of ProcGlobalSemas
int ProcGlobalSemas(void) {
    // Calculate total semaphores needed:
    // - One semaphore per backend process (including autovacuum)
    // - One semaphore per auxiliary process (checkpointer, WAL writer, etc.)
    return MaxBackends + NUM_AUXILIARY_PROCS;
}
```

Key simplifications made:
- Added explanatory comments for clarity
- Maintained the original simple logic (no simplification needed as function is already minimal)
- Focused on the core purpose: counting required semaphores for process synchronization