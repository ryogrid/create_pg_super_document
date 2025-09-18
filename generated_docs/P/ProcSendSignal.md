# ProcSendSignal

## Location
src/backend/storage/lmgr/proc.c: 1883 - 1897

## Overview
Sets the latch of a backend process identified by its ProcNumber to signal that process to wake up from waiting.

## Definition
void ProcSendSignal(ProcNumber procNumber)

## Detailed Description
ProcSendSignal provides a mechanism to signal a specific backend process by setting its latch. The function takes a ProcNumber (process identifier) and sets the corresponding process's latch in the global process array. This is the counterpart to ProcWaitForSignal, enabling one process to wake up another process that is waiting. The function includes bounds checking to ensure the ProcNumber is valid within the range of existing processes, throwing an error if the ProcNumber is out of range.

## Parameters / Member Variables
- : The process number identifying the target backend process to signal (must be within valid range)

## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md)
  - ProcNumber (type reference)
- Called from (representative examples):
  - UnpinBufferNoOwner
  - [ReleasePredicateLocks](../R/ReleasePredicateLocks.md)

## Notes and Other Information
- Performs bounds checking against ProcGlobal->allProcCount to prevent invalid access
- Uses the global process array (ProcGlobal->allProcs) to locate the target process
- Sets the procLatch field of the target process's PGPROC structure
- Complementary function to ProcWaitForSignal for inter-process communication
- Throws an ERROR if the procNumber is negative or exceeds the process count
- Essential for coordinating between backends in scenarios like buffer management and lock cleanup