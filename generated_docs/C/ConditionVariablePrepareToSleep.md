# ConditionVariablePrepareToSleep

## Location
[src/backend/storage/lmgr/condition_variable.c:56-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/condition_variable.c#L56-L95)

## Overview
Prepares the current process to wait on a condition variable by adding it to the wait queue, optimizing efficiency for scenarios where the process will likely need to sleep.

## Definition
```c
void ConditionVariablePrepareToSleep(ConditionVariable *cv)
```

## Detailed Description
ConditionVariablePrepareToSleep is an optional optimization function that prepares the current process to wait on a condition variable. It adds the process to the condition variable's wait queue before entering a test/sleep loop. This is more efficient when the process is expected to sleep at least once, as it avoids the overhead of repeatedly adding/removing the process from the wait queue.

The function handles the case where another sleep is already prepared by canceling it first, since PostgreSQL maintains only one static variable for tracking prepared sleeps per process. When called, it records the target condition variable and adds the current process (identified by MyProcNumber) to the wait queue under spinlock protection.

A critical requirement is that the exit condition must be tested between calling this function and ConditionVariableSleep, as the process is already in the wait queue and could be signaled.

## Parameters / Member Variables
- `cv`: Pointer to the ConditionVariable on which the process will wait

## Dependencies
- Functions called/Symbols referenced:
  - [ConditionVariableCancelSleep](ConditionVariableCancelSleep.md) (cancels any previous prepared sleep)
  - proclist_push_tail (adds process to wait queue)
  - SpinLockAcquire/SpinLockRelease (protects wait queue modifications)
- Called from (representative examples):
  - Checkpointer signal handling
  - [ReplicationSlotAcquire](../R/ReplicationSlotAcquire.md)
  - [InvalidatePossiblyObsoleteSlot](../I/InvalidatePossiblyObsoleteSlot.md)
  - [WaitForStandbyConfirmation](../W/WaitForStandbyConfirmation.md)
  - [ShutdownWalRcv](../S/ShutdownWalRcv.md)
  - [WalSndWait](../W/WalSndWait.md)
  - [WaitIO](../W/WaitIO.md)
  - [BarrierArriveAndWait](../B/BarrierArriveAndWait.md)
  - [ConditionVariableTimedSleep](ConditionVariableTimedSleep.md)
  - [injection_wait](../i/injection_wait.md)

## Notes and Other Information
- Optional optimization - can be omitted if first test of exit condition is likely to succeed
- Must test exit condition between this call and ConditionVariableSleep
- Automatically cancels any previous prepared sleep for the same process
- Uses MyProcNumber to identify the current process
- More efficient for scenarios expecting multiple sleep cycles
- Process remains in wait queue until explicitly removed by sleep/cancel operations

## Simplified Source

```c
// Simplified version of ConditionVariablePrepareToSleep
void ConditionVariablePrepareToSleep(ConditionVariable *cv) {
    int pgprocno = MyProcNumber;

    // Cancel any previous prepared sleep for this process
    if (cv_sleep_target != NULL) {
        ConditionVariableCancelSleep();
    }

    // Record which condition variable we'll sleep on
    cv_sleep_target = cv;

    // Add this process to the condition variable's wait queue
    SpinLockAcquire(&cv->mutex);
    proclist_push_tail(&cv->wakeup, pgprocno, cvWaitLink);
    SpinLockRelease(&cv->mutex);
}
```

Key simplifications made:
- Removed detailed comments explaining the rationale (preserved in main docs)
- Kept essential logic flow: cancel previous sleep, record target, add to queue
- Maintained critical spinlock protection around queue modification
- Preserved all functional behavior while making code more readable
- Condensed variable usage explanation into brief inline comments