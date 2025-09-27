# ConditionVariableBroadcast

## Location
[src/backend/storage/lmgr/condition_variable.c:282-360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/condition_variable.c#L282-L360)

## Overview
Wakes up all processes sleeping on a condition variable at the time of call, implementing a "broadcast" or "signal all" operation.

## Definition
void ConditionVariableBroadcast(ConditionVariable *cv)

## Detailed Description
ConditionVariableBroadcast implements a broadcast operation that wakes up all processes currently sleeping on the given condition variable. This function guarantees to wake all processes that were sleeping on the CV at the time of call, but processes that add themselves to the list during the execution may not be awakened.

The function uses a sophisticated sentinel-based approach to handle the case where awakened processes might immediately re-queue themselves. It inserts its own process entry as a sentinel in the wakeup queue to detect when all originally waiting processes have been processed. This prevents infinite loops when processes re-add themselves to the queue after being awakened.

The implementation handles edge cases carefully:
- If there's exactly one entry, it simply removes and signals that entry
- For multiple entries, it uses a sentinel mechanism to ensure all original waiters are awakened
- It properly handles the case where another process might remove the sentinel entry
- It cancels any existing CV sleep state before proceeding

## Parameters / Member Variables
- `cv`: Pointer to the ConditionVariable structure containing the wakeup queue and synchronization mutex

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [proclist_is_empty](../p/proclist_is_empty.md)
  - proclist_pop_head_node
  - proclist_push_tail
  - proclist_contains
  - [ConditionVariableCancelSleep](ConditionVariableCancelSleep.md)
  - [SetLatch](../S/SetLatch.md)
- Called from (representative examples):
  - [_bt_parallel_done](../b/_bt_parallel_done.md)
  - [RecordNewMultiXact](../R/RecordNewMultiXact.md)
  - [SetRecoveryPause](../S/SetRecoveryPause.md)
  - [BitmapDoneInitializingSharedState](../B/BitmapDoneInitializingSharedState.md)
  - [CheckpointerMain](CheckpointerMain.md)
  - [WalSummarizerMain](../W/WalSummarizerMain.md)
  - [ReplicationSlotCreate](../R/ReplicationSlotCreate.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [BarrierArriveAndWait](../B/BarrierArriveAndWait.md)

## Notes and Other Information
- Uses a sentinel-based algorithm to prevent infinite loops when awakened processes immediately re-queue themselves
- Guarantees awakening all processes that were waiting at call time, but not those added during execution
- Handles concurrent modifications to the wakeup queue safely through spinlock protection
- May produce spurious wakeups in some edge cases, which is harmless but slightly inefficient
- Automatically cancels any existing CV sleep state to avoid conflicts with the sentinel mechanism
- Widely used throughout PostgreSQL for synchronization in parallel operations, checkpointing, replication, and buffer management

## Simplified Source

```c
// Simplified version of ConditionVariableBroadcast
void ConditionVariableBroadcast(ConditionVariable *cv) {
    int pgprocno = MyProcNumber;
    PGPROC *proc = NULL;
    bool have_sentinel = false;

    // Step 1: Cancel any existing CV sleep to avoid conflicts
    if (cv_sleep_target != NULL)
        ConditionVariableCancelSleep();

    // Step 2: Check the wakeup queue and handle based on its state
    SpinLockAcquire(&cv->mutex);

    if (!proclist_is_empty(&cv->wakeup)) {
        // Remove the first waiter from the queue
        proc = proclist_pop_head_node(&cv->wakeup, cvWaitLink);

        // If more waiters exist, add ourselves as sentinel to track progress
        if (!proclist_is_empty(&cv->wakeup)) {
            proclist_push_tail(&cv->wakeup, pgprocno, cvWaitLink);
            have_sentinel = true;
        }
    }

    SpinLockRelease(&cv->mutex);

    // Step 3: Wake up the first waiter
    if (proc != NULL)
        SetLatch(&proc->procLatch);

    // Step 4: Continue waking waiters until our sentinel is removed
    while (have_sentinel) {
        proc = NULL;

        // Get next waiter from the queue
        SpinLockAcquire(&cv->mutex);
        if (!proclist_is_empty(&cv->wakeup))
            proc = proclist_pop_head_node(&cv->wakeup, cvWaitLink);

        // Check if our sentinel is still in the queue
        have_sentinel = proclist_contains(&cv->wakeup, pgprocno, cvWaitLink);
        SpinLockRelease(&cv->mutex);

        // Wake up the process (but not ourselves)
        if (proc != NULL && proc != MyProc)
            SetLatch(&proc->procLatch);
    }
}
```

Key simplifications made:
- Removed detailed comments explaining edge cases and algorithm rationale
- Consolidated the main logic into clear sequential steps
- Abstracted the sentinel mechanism explanation into brief comments
- Focused on the core execution flow rather than implementation details
- Preserved the essential algorithm structure and correctness