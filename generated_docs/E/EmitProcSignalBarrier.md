# EmitProcSignalBarrier

## Location
[src/backend/storage/ipc/procsignal.c:329-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procsignal.c#L329-L388)

## Overview
Sends a signal to every PostgreSQL process and returns a barrier generation number that can be used to wait until all processes have absorbed the signal or started afterwards.

## Definition
```c
uint64 EmitProcSignalBarrier(ProcSignalBarrierType type)
```

## Detailed Description
EmitProcSignalBarrier is a critical synchronization mechanism that broadcasts a signal to all active PostgreSQL backend processes. The function operates in three phases:

1. **Flag Setting**: Sets barrier check flags for all process signal slots using atomic operations to ensure thread safety and memory ordering guarantees.

2. **Generation Increment**: Atomically increments the global barrier generation counter, which serves as a unique identifier for this barrier operation.

3. **Signal Broadcasting**: Sends SIGUSR1 signals to all active processes, causing them to update their advertised barrier generation and process any pending barrier-related work.

The function is designed to be safe from exceptions (will not throw ERROR or FATAL) and uses full memory barrier semantics to ensure proper ordering of operations. It's intended for infrequent use due to the performance impact of interrupting all backend processes.

## Parameters / Member Variables
- `type`: The type of barrier signal to emit, specified by ProcSignalBarrierType enum

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_fetch_or_u32](../p/pg_atomic_fetch_or_u32.md) (atomic bitwise OR operation)
  - [pg_atomic_add_fetch_u64](../p/pg_atomic_add_fetch_u64.md) (atomic increment operation)
  - kill (system call to send signals)
- Data structures accessed:
  - ProcSignal global structure
  - [ProcSignalSlot](../P/ProcSignalSlot.md) array elements
  - NumProcSignalSlots global variable
- Constants used:
  - PROCSIG_BARRIER (signal type identifier)
  - SIGUSR1 (Unix signal number)

- Called from (representative examples):
  - [dropdb](../d/dropdb.md) (database drop operations)
  - [movedb](../m/movedb.md) (database move operations)
  - [dbase_redo](../d/dbase_redo.md) (database WAL replay)
  - [DropTableSpace](../D/DropTableSpace.md) (tablespace removal)
  - [tblspc_redo](../t/tblspc_redo.md) (tablespace WAL replay)

## Notes and Other Information
- The function returns a generation number that can be passed to WaitForProcSignalBarrier to synchronize on completion
- Uses atomic operations with full barrier semantics for thread safety
- Processes slots in reverse order during signaling phase for implementation efficiency
- New backends that join during execution automatically have current state and don't need special handling
- The barrier mechanism is heavyweight and should be used sparingly due to performance implications
- Located in src/backend/storage/ipc/procsignal.c:329-388

## Simplified Source

```c
uint64 EmitProcSignalBarrier(ProcSignalBarrierType type) {
    uint32 flagbit = 1 << (uint32) type;
    uint64 generation;

    // Step 1: Set barrier check flags for all process slots
    // Using atomic operations with full barrier semantics
    for (int i = 0; i < NumProcSignalSlots; i++) {
        volatile ProcSignalSlot *slot = &ProcSignal->psh_slot[i];

        // Atomically set the flag bit for this barrier type
        pg_atomic_fetch_or_u32(&slot->pss_barrierCheckMask, flagbit);
    }

    // Step 2: Increment the global barrier generation counter
    // This creates a unique identifier for this barrier operation
    generation = pg_atomic_add_fetch_u64(&ProcSignal->psh_barrierGeneration, 1);

    // Step 3: Signal all active processes to process the barrier
    // Process slots in reverse order for implementation efficiency
    for (int i = NumProcSignalSlots - 1; i >= 0; i--) {
        volatile ProcSignalSlot *slot = &ProcSignal->psh_slot[i];
        pid_t pid = slot->pss_pid;

        if (pid != 0) {
            // Set the barrier signal flag and send SIGUSR1
            slot->pss_signalFlags[PROCSIG_BARRIER] = true;
            kill(pid, SIGUSR1);
        }
    }

    // Return generation number for use with WaitForProcSignalBarrier
    return generation;
}
```