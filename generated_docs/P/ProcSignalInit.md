# ProcSignalInit

## Location
[src/backend/storage/ipc/procsignal.c:158-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procsignal.c#L158-L210)

## Overview
Registers the current process in the ProcSignal array by claiming and initializing a process signal slot, setting up the process for inter-process communication and barrier synchronization.

## Definition

```c
void
ProcSignalInit(void)
```
## Detailed Description
ProcSignalInit is called during process initialization to claim a process signal slot and register the current process in the shared memory process signaling infrastructure. The function validates that MyProcNumber is properly set and within valid bounds, then initializes the corresponding slot with the current process ID. It clears any leftover signal flags, initializes barrier synchronization state by reading the current barrier generation and clearing check mask bits, and sets up cleanup to be performed on process exit. The function includes careful memory barrier usage to ensure proper ordering of memory operations during initialization.

## Parameters / Member Variables
This function takes no parameters but relies on global variables:
- MyProcNumber: The slot index assigned to this process
- MyProcPid: The current process ID
- ProcSignal: Pointer to shared memory process signal structure

## Dependencies
- Functions called/Symbols referenced:
  - elog
  - MemSet
  - [pg_atomic_write_u32](../p/pg_atomic_write_u32.md)
  - [pg_atomic_read_u64](../p/pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - pg_memory_barrier
  - [on_shmem_exit](../o/on_shmem_exit.md)
  - [CleanupProcSignalState](../C/CleanupProcSignalState.md)
  - [ProcSignalSlot](ProcSignalSlot.md) (type)
  - NUM_PROCSIGNALS (constant)
  - NumProcSignalSlots (variable)
- Called from (representative examples):
  - [InitPostgres](../I/InitPostgres.md)
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)

## Notes and Other Information
- Must be called early in process initialization before caching any state that might need invalidation
- Validates MyProcNumber is set and within valid range (0 to NumProcSignalSlots-1)
- Warns if taking over a slot that appears to still be in use by another process
- Uses atomic operations and memory barriers for thread-safe barrier state initialization
- Automatically registers CleanupProcSignalState to run on process exit
- Sets MyProcSignalSlot global variable for use by CheckProcSignal
- Located in src/backend/storage/ipc/procsignal.c:158-210

## Simplified Source

```c
// Simplified version of ProcSignalInit
void ProcSignalInit(void) {
    ProcSignalSlot *slot;
    uint64 barrier_generation;

    // Validate process number is set and within bounds
    if (MyProcNumber < 0)
        elog(ERROR, "MyProcNumber not set");
    if (MyProcNumber >= NumProcSignalSlots)
        elog(ERROR, "unexpected MyProcNumber %d in ProcSignalInit (max %d)",
             MyProcNumber, NumProcSignalSlots);

    // Get pointer to our slot in the shared array
    slot = &ProcSignal->psh_slot[MyProcNumber];

    // Warn if slot appears to be already in use
    if (slot->pss_pid != 0)
        elog(LOG, "process %d taking over ProcSignal slot %d, but it's not empty",
             MyProcPid, MyProcNumber);

    // Clear any leftover signal flags from previous use
    MemSet(slot->pss_signalFlags, 0, NUM_PROCSIGNALS * sizeof(sig_atomic_t));

    // Initialize barrier synchronization state
    // Clear check mask and sync with current barrier generation
    pg_atomic_write_u32(&slot->pss_barrierCheckMask, 0);
    barrier_generation = pg_atomic_read_u64(&ProcSignal->psh_barrierGeneration);
    pg_atomic_write_u64(&slot->pss_barrierGeneration, barrier_generation);
    pg_memory_barrier();

    // Mark slot as owned by this process
    slot->pss_pid = MyProcPid;

    // Set global pointer for use by other functions
    MyProcSignalSlot = slot;

    // Register cleanup function to release slot on exit
    on_shmem_exit(CleanupProcSignalState, (Datum) 0);
}
```

Key simplifications made:
- Preserved all essential validation and error handling
- Kept critical atomic operations and memory barriers intact
- Added clear comments for each major step
- Maintained the exact logic flow and memory safety guarantees
- Focused on the core functionality: claim slot, initialize state, register cleanup