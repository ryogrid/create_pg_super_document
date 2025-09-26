# ConditionVariableBroadcast

## Location
src/backend/storage/lmgr/condition_variable.c: 282 - 360

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
  - proclist_is_empty
  - proclist_pop_head_node
  - proclist_push_tail
  - proclist_contains
  - ConditionVariableCancelSleep
  - SetLatch
- Called from (representative examples):
  - _bt_parallel_done
  - RecordNewMultiXact
  - SetRecoveryPause
  - BitmapDoneInitializingSharedState
  - CheckpointerMain
  - WalSummarizerMain
  - ReplicationSlotCreate
  - WalReceiverMain
  - BarrierArriveAndWait

## Notes and Other Information
- Uses a sentinel-based algorithm to prevent infinite loops when awakened processes immediately re-queue themselves
- Guarantees awakening all processes that were waiting at call time, but not those added during execution
- Handles concurrent modifications to the wakeup queue safely through spinlock protection
- May produce spurious wakeups in some edge cases, which is harmless but slightly inefficient
- Automatically cancels any existing CV sleep state to avoid conflicts with the sentinel mechanism
- Widely used throughout PostgreSQL for synchronization in parallel operations, checkpointing, replication, and buffer management