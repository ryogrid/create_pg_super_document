# EmitProcSignalBarrier

## Location
src/backend/storage/ipc/procsignal.c: 329 - 388

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