# CheckProcSignal

## Location
src/backend/storage/ipc/procsignal.c: 614 - 634

## Overview
CheckProcSignal is a static utility function that checks if a specific process signal reason has been signaled and clears the signal flag atomically.

## Definition


## Detailed Description
CheckProcSignal is a core function in PostgreSQL's inter-process signaling mechanism that safely checks and clears signal flags stored in shared memory. The function is designed to be called after a process receives SIGUSR1 to determine which specific signal reason triggered the interrupt. It operates on the current process's signal slot (MyProcSignalSlot) and uses a careful approach to avoid race conditions by only clearing flags that have actually been observed as set.

The function implements an atomic test-and-clear operation on signal flags, ensuring that each signal is processed exactly once. This is crucial for PostgreSQL's reliable inter-process communication system where signals coordinate activities like recovery conflicts, parallel processing, and various maintenance operations.

## Parameters / Member Variables
- : A ProcSignalReason enum value specifying which specific signal type to check for (e.g., PROCSIG_CATCHUP_INTERRUPT, PROCSIG_NOTIFY_INTERRUPT, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - ProcSignalReason (enum type)
  - [ProcSignalSlot](../P/ProcSignalSlot.md) (struct type)
  - MyProcSignalSlot (global variable)
- Called from (representative examples):
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md) (primary caller, checks multiple signal reasons)

## Notes and Other Information
- The function is static, meaning it's only accessible within the procsignal.c compilation unit
- Returns true if the specified signal reason was flagged and has been cleared, false otherwise
- The careful flag clearing logic ("don't clear flag if we haven't seen it set") prevents race conditions
- If MyProcSignalSlot is NULL, the function safely returns false without error
- This function is central to PostgreSQL's signal dispatching mechanism and is called repeatedly by procsignal_sigusr1_handler to handle different types of inter-process notifications