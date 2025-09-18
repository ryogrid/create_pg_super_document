# PMSignalData

## Location
src/backend/storage/ipc/pmsignal.c: 71 - 100

## Overview
PMSignalData is a shared memory structure that facilitates inter-process communication between the PostgreSQL postmaster and its child processes, storing signal flags and quit reasons for process coordination.

## Definition


## Detailed Description
PMSignalData serves as the central communication hub in PostgreSQL's multi-process architecture, residing in shared memory to enable signal-based coordination between the postmaster and its child processes. The structure is designed as an opaque type with its implementation details hidden within pmsignal.c, providing a clean interface for process management operations.

The structure supports bidirectional communication: child processes can signal the postmaster about various events (recovery started, autovacuum requests, etc.), while the postmaster can send quit signals to children with specific reasons (crash recovery, immediate shutdown). The use of sig_atomic_t ensures that signal operations are atomic and safe for use in signal handlers.

## Parameters / Member Variables
- : Array of atomic flags used by child processes to signal specific events to the postmaster, with each index corresponding to a different signal reason defined in the PMSignalReason enum
- : Stores the reason why SIGQUIT was sent from postmaster to children, using values from the QuitSignalReason enum (crash recovery, immediate stop, or not sent)
- : Count of entries in the PMChildFlags array, indicating how many child processes can be tracked
- : Variable-length array of atomic flags for per-child-process signaling, allowing the postmaster to send signals to individual child processes

## Dependencies
- Functions called/Symbols referenced:
  - NUM_PMSIGNALS (enum value defining array size)
  - QuitSignalReason (enum type for quit signal reasons)
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length arrays)
- Called from (representative examples):
  - PMSignalShmemSize (calculates shared memory size needed)
  - PMSignalShmemInit (initializes the structure in shared memory)
  - SubPostmasterMain (accesses signal data in child processes)

## Notes and Other Information
- The structure is allocated in shared memory to enable inter-process communication
- Declared as an opaque type in pmsignal.h with implementation details hidden in pmsignal.c
- Uses sig_atomic_t for thread and signal safety in concurrent access scenarios
- The flexible array member allows dynamic sizing based on the maximum number of child processes
- Part of PostgreSQL's signal-based IPC mechanism alongside pipes and shared memory
- Critical for proper shutdown sequences and crash recovery coordination