# ShutDownSlotSync

## Location
[src/backend/replication/logical/slotsync.c:1562-1629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1562-L1629)

## Overview
Gracefully shuts down the slot synchronization worker and waits for all slot synchronization activities to complete before updating synchronized slot timestamps.

## Definition

```c
void
ShutDownSlotSync(void)
```
## Detailed Description
This function is responsible for the coordinated shutdown of PostgreSQL's slot synchronization infrastructure. It signals the slot sync worker to stop, waits for any in-progress synchronization operations to complete, and ensures proper cleanup of slot state. The function handles both the slot sync worker process and any running pg_sync_replication_slots() function calls. After confirming that all synchronization activities have ceased, it updates the inactive_since timestamps for synchronized slots to maintain accurate slot state information. The function uses a polling mechanism with latches to efficiently wait for shutdown completion while remaining responsive to interrupts.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease (thread-safe access to SlotSyncCtx)
  - kill (sends SIGINT to worker process)
  - [WaitLatch](../W/WaitLatch.md) (efficient waiting with timeout and interrupt handling)
  - [ResetLatch](../R/ResetLatch.md) (latch state management)
  - CHECK_FOR_INTERRUPTS (interrupt processing)
  - [update_synced_slots_inactive_since](../u/update_synced_slots_inactive_since.md) (slot timestamp updates)
  - SlotSyncCtx (global slot synchronization context)
  - InvalidPid (process ID validation)
  - MyLatch (current process latch)

- Called from (representative examples):
  - [FinishWalRecovery](../F/FinishWalRecovery.md) (src/backend/access/transam/xlogrecovery.c:1486)
  - Referenced in SLOTSYNC_H header (src/include/replication/slotsync.h:31)

## Notes and Other Information
- Sets the stopSignaled flag to indicate shutdown is requested
- Sends SIGINT to the worker process if it's currently running
- Uses a polling loop with WaitLatch for efficient waiting with 10ms timeout
- Handles both worker process shutdown and function-based synchronization completion
- Always calls update_synced_slots_inactive_since() before returning to ensure slot state consistency
- Part of the PostgreSQL recovery and promotion process infrastructure
- Critical for clean server shutdown and standby-to-primary promotion scenarios
- Uses proper locking to ensure thread-safe access to shared synchronization state
- Responsive to process interrupts during the shutdown waiting period

## Simplified Source

```c
// Simplified version of ShutDownSlotSync
void ShutDownSlotSync(void) {
    pid_t worker_pid;

    // Step 1: Signal shutdown and check if sync is active
    SpinLockAcquire(&SlotSyncCtx->mutex);
    SlotSyncCtx->stopSignaled = true;

    if (!SlotSyncCtx->syncing) {
        // No sync activity - just update timestamps and return
        SpinLockRelease(&SlotSyncCtx->mutex);
        update_synced_slots_inactive_since();
        return;
    }

    worker_pid = SlotSyncCtx->pid;
    SpinLockRelease(&SlotSyncCtx->mutex);

    // Step 2: Send shutdown signal to worker process
    if (worker_pid != InvalidPid) {
        kill(worker_pid, SIGINT);
    }

    // Step 3: Wait for synchronization to complete
    for (;;) {
        // Wait with timeout and interrupt handling
        int rc = WaitLatch(MyLatch,
                          WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                          10L, WAIT_EVENT_REPLICATION_SLOTSYNC_SHUTDOWN);

        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }

        // Check if synchronization has stopped
        SpinLockAcquire(&SlotSyncCtx->mutex);
        if (!SlotSyncCtx->syncing) {
            break;  // Sync completed - exit loop
        }
        SpinLockRelease(&SlotSyncCtx->mutex);
    }

    SpinLockRelease(&SlotSyncCtx->mutex);

    // Step 4: Update slot timestamps after shutdown
    update_synced_slots_inactive_since();
}
```

Key simplifications made:
- Added clear step-by-step comments for main logic flow
- Simplified variable declarations and initialization
- Consolidated latch waiting logic into a single clear block
- Emphasized the three main phases: signal, wait, and cleanup
- Preserved all essential functionality while improving readability
- Maintained proper error handling and interrupt processing