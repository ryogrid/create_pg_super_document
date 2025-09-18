# ProcSignalSlot

## Location
[src/backend/storage/ipc/procsignal.c:70-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procsignal.c#L70-L81)

## Overview
ProcSignalSlot is a shared memory structure that enables inter-process communication in PostgreSQL by allowing processes to signal each other for various reasons without requiring explicit locking.

## Definition


## Detailed Description
ProcSignalSlot is a core component of PostgreSQL's inter-process signaling system that multiplexes SIGUSR1 signals to support multiple concurrent event types. Each PostgreSQL process (backend or auxiliary process like checkpointer) that wants to receive signals registers its process ID in a ProcSignalSlots array, which is indexed by ProcNumber for efficient slot allocation and signal targeting.

The structure uses atomic operations and volatile declarations to ensure thread-safe communication without explicit locking. It supports two types of signaling: simple fire-and-forget signals via pss_signalFlags, and barrier-based signaling that requires confirmation from all processes before proceeding with global state changes.

The barrier mechanism ensures that critical system-wide operations (like file closure requests) are acknowledged by all processes before being considered complete, providing synchronization guarantees across the entire PostgreSQL cluster.

## Parameters / Member Variables
- : The process ID of the PostgreSQL process that owns this slot, declared volatile for atomic access
- : Array of volatile atomic flags, one for each signal reason (catchup interrupt, notify interrupt, parallel message, etc.), allowing concurrent signaling of different reasons
- : Atomic 64-bit counter tracking the current barrier generation number for this process, used to confirm receipt of barrier signals
- : Atomic 32-bit bitmask indicating which barrier types this process should check for, with each bit representing a different barrier type
- : Condition variable used for efficient waiting on barrier completion, avoiding busy polling

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md) (for barrier generation tracking)
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md) (for barrier check mask)
  - ConditionVariable (for barrier synchronization)
  - sig_atomic_t (for atomic signal flags)
  - NUM_PROCSIGNALS (enum constant defining number of signal types)
- Called from (representative examples):
  - [ProcSignalShmemInit](ProcSignalShmemInit.md) (initialization of slot arrays)
  - [ProcSignalInit](ProcSignalInit.md) (process registration)
  - [SendProcSignal](../S/SendProcSignal.md) (signal sending)
  - [EmitProcSignalBarrier](../E/EmitProcSignalBarrier.md) (barrier emission)
  - [WaitForProcSignalBarrier](../W/WaitForProcSignalBarrier.md) (barrier waiting)
  - [CheckProcSignal](../C/CheckProcSignal.md) (signal checking)

## Notes and Other Information
- The structure is designed for maximum portability using volatile sig_atomic_t declarations to ensure atomic loads and stores without explicit locking
- Each process is allocated exactly one slot in the shared memory array, indexed by its ProcNumber
- The barrier mechanism is critical for operations that require cluster-wide acknowledgment, such as storage manager file closure requests (PROCSIGNAL_BARRIER_SMGRRELEASE)
- Signal flags support concurrent signaling of different reasons, but multiple signals of the same reason may be coalesced into a single observation
- The system is designed to be safe even if processes mistakenly receive signals due to race conditions
- Total number of slots is calculated as MaxBackends + NUM_AUXILIARY_PROCS to accommodate all possible PostgreSQL processes