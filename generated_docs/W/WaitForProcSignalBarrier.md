# WaitForProcSignalBarrier

## Location
[src/backend/storage/ipc/procsignal.c:389-447](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procsignal.c#L389-L447)

## Overview
Waits until it is guaranteed that all changes requested by a specific call to EmitProcSignalBarrier() have taken effect across all PostgreSQL backend processes.

## Definition
```c
void WaitForProcSignalBarrier(uint64 generation)
```

## Detailed Description
WaitForProcSignalBarrier provides a synchronization mechanism that allows a process to wait until all active PostgreSQL backends have processed a specific barrier generation. The function operates by:

1. **Generation Validation**: Asserts that the specified generation is not greater than the current barrier generation, ensuring the request is valid.

2. **Slot Monitoring**: Iterates through all process signal slots in reverse order, checking each backend's barrier generation progress.

3. **Conditional Waiting**: For each slot where the backend hasn't yet processed the target generation, it uses a condition variable with a 5-second timeout to wait for updates. If the timeout expires, it logs a warning message about the slow backend.

4. **Memory Barrier**: After all backends have caught up, inserts a memory barrier to ensure proper ordering of subsequent shared memory operations.

The function specifically monitors pss_barrierGeneration rather than pss_barrierCheckMask because the generation is updated only after the barrier has been fully absorbed, providing stronger guarantees about completion.

## Parameters / Member Variables
- `generation`: The barrier generation number (returned by EmitProcSignalBarrier) to wait for

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md) (atomic read operations)
  - [ConditionVariableTimedSleep](../C/ConditionVariableTimedSleep.md) (timed condition variable wait)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md) (condition variable cleanup)
  - pg_memory_barrier (memory synchronization)
  - elog (logging function)
  - ereport (error reporting)
- Data structures accessed:
  - ProcSignal global structure
  - [ProcSignalSlot](../P/ProcSignalSlot.md) array elements  
  - NumProcSignalSlots global variable
- Constants used:
  - DEBUG1, LOG (logging levels)
  - UINT64_FORMAT (printf format specifier)
  - WAIT_EVENT_PROC_SIGNAL_BARRIER (wait event type)

- Called from (representative examples):
  - [dropdb](../d/dropdb.md) (database drop operations)
  - [movedb](../m/movedb.md) (database move operations)
  - [dbase_redo](../d/dbase_redo.md) (database WAL replay)
  - [DropTableSpace](../D/DropTableSpace.md) (tablespace removal)
  - [tblspc_redo](../t/tblspc_redo.md) (tablespace WAL replay)

## Notes and Other Information
- Uses condition variables with 5-second timeouts to avoid indefinite blocking
- Logs warnings for backends that take longer than 5 seconds to process barriers
- Includes explicit memory barrier at the end to ensure proper ordering with subsequent operations
- Processes slots in reverse order for consistency with EmitProcSignalBarrier
- The function is designed to be robust against slow or unresponsive backends
- Debug logging helps track barrier synchronization progress
- Located in src/backend/storage/ipc/procsignal.c:389-447

## Simplified Source

```c
void WaitForProcSignalBarrier(uint64 generation) {
    // Validate that generation is not beyond current barrier generation
    Assert(generation <= pg_atomic_read_u64(&ProcSignal->psh_barrierGeneration));

    elog(DEBUG1, "waiting for all backends to process ProcSignalBarrier generation " UINT64_FORMAT, generation);

    // Step 1: Check each backend slot in reverse order
    for (int i = NumProcSignalSlots - 1; i >= 0; i--) {
        ProcSignalSlot *slot = &ProcSignal->psh_slot[i];
        uint64 oldval;

        // Step 2: Monitor barrier generation for this backend
        // Use pss_barrierGeneration (not pss_barrierCheckMask) for strong guarantee
        oldval = pg_atomic_read_u64(&slot->pss_barrierGeneration);

        // Step 3: Wait until backend processes the target generation
        while (oldval < generation) {
            // Wait up to 5 seconds, then log warning for slow backends
            if (ConditionVariableTimedSleep(&slot->pss_barrierCV, 5000,
                                           WAIT_EVENT_PROC_SIGNAL_BARRIER)) {
                ereport(LOG, (errmsg("still waiting for backend with PID %d to accept ProcSignalBarrier",
                                    (int) slot->pss_pid)));
            }

            // Re-read generation value after wait
            oldval = pg_atomic_read_u64(&slot->pss_barrierGeneration);
        }

        // Clean up condition variable wait state
        ConditionVariableCancelSleep();
    }

    elog(DEBUG1, "finished waiting for all backends to process ProcSignalBarrier generation " UINT64_FORMAT, generation);

    // Step 4: Insert memory barrier to separate barrier check from subsequent operations
    // This ensures proper ordering for shared state access after barrier completion
    pg_memory_barrier();
}
```