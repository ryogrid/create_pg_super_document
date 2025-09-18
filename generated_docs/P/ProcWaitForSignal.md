# ProcWaitForSignal

## Location
src/backend/storage/lmgr/proc.c: 1871 - 1882

## Overview
Waits for a signal from another backend using the generic process latch mechanism, providing a standardized way for processes to block until signaled by other processes.

## Definition
void ProcWaitForSignal(uint32 wait_event_info)

## Detailed Description
ProcWaitForSignal implements a generic inter-process signaling mechanism using PostgreSQL's latch system. The function blocks the current process until it receives a signal from another backend, typically used for coordination between processes. It uses WaitLatch with flags to wait for latch activation while also monitoring for postmaster death. After being awakened, it resets the latch to prepare for future signals and checks for any pending interrupts. The caller must be prepared for spurious wakeups and should always verify that the desired condition has been met before proceeding.

## Parameters / Member Variables
- : A 32-bit value identifying the type of wait event for monitoring and diagnostic purposes

## Dependencies
- Functions called/Symbols referenced:
  - [WaitLatch](../W/WaitLatch.md)
  - [ResetLatch](../R/ResetLatch.md)
  - CHECK_FOR_INTERRUPTS
  - WL_LATCH_SET
  - WL_EXIT_ON_PM_DEATH
- Called from (representative examples):
  - LockBufferForCleanup
  - ResolveRecoveryConflictWithLock
  - ResolveRecoveryConflictWithBufferPin
  - GetSafeSnapshot

## Notes and Other Information
- Uses the generic process latch (MyLatch) which can receive unrelated wakeups
- Callers must be robust against spurious wakeups and should always verify conditions
- Always resets the latch after waking to prepare for future signals
- Includes CHECK_FOR_INTERRUPTS() to handle pending signals and cancellations
- The wait_event_info parameter enables monitoring of what the process is waiting for
- Automatically exits if the postmaster dies (WL_EXIT_ON_PM_DEATH flag)