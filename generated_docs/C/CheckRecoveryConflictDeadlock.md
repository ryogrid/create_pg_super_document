# CheckRecoveryConflictDeadlock

## Location
src/backend/storage/ipc/standby.c: 904 - 934

## Overview
Performs early deadlock detection in Hot Standby by checking if the current process is about to sleep while holding buffer pins that the startup process needs.

## Definition


## Detailed Description
This function implements a pessimistic early deadlock detection mechanism specifically for Hot Standby scenarios. It prevents deadlocks that can occur when a user transaction holds buffer pins that the startup process needs, while simultaneously trying to wait for locks that can only be cleared by the startup process completing its recovery work.

The function operates on a simple principle: if the current process is about to go to sleep waiting for a lock, but it's also holding buffer pins that are delaying recovery, then there's a potential for deadlock. Rather than wait and potentially create an actual deadlock, the function proactively cancels the current transaction with an appropriate error message.

The detection is pessimistic because it cannot determine whether the lock being waited for is actually related to what the startup process holds. This means some transactions may be canceled unnecessarily, but this is considered acceptable given the low probability of such cases and the complexity that would be required for more precise detection.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - HoldingBufferPinThatDelaysRecovery (checks if current process holds problematic pins)
  - ereport (reports the error and cancels the transaction)
- Called from (representative examples):
  - ProcSleep (src/backend/storage/lmgr/proc.c:1238)

## Notes and Other Information
- Only called from non-startup processes (asserts !InRecovery)
- Uses pessimistic detection - may cancel transactions that wouldn't actually deadlock
- Error message matches ProcessInterrupts() for consistency but avoids calling that function
- Only cancels the current transaction, not parent transactions in subtransaction scenarios
- Comments indicate this mechanism should eventually be replaced with buffer lock accounting in DeadLockCheck()
- Part of the broader recovery conflict resolution system in PostgreSQL Hot Standby
- Reports ERRCODE_T_R_DEADLOCK_DETECTED error code when canceling transactions
- The error message specifically mentions 'buffer deadlock with recovery' to distinguish from regular deadlocks
- Low-probability errors in practice, making the current simple approach acceptable for now