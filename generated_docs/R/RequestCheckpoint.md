# RequestCheckpoint

## Location
[src/backend/postmaster/checkpointer.c:947-996](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L947-L996)

## Overview
Initiates a checkpoint request from backend processes, either executing it immediately in standalone mode or signaling the checkpointer process in normal operation.

## Definition
```c
void RequestCheckpoint(int flags)
```

## Detailed Description
This function provides the main interface for backend processes to request database checkpoints. It handles two distinct execution paths:

1. **Standalone mode**: When not in a postmaster environment, it directly calls CreateCheckPoint() with CHECKPOINT_IMMEDIATE flag and performs cleanup.

2. **Normal operation**: In a multi-process environment, it:
   - Atomically sets checkpoint flags in shared memory using bitwise OR to preserve stronger requests
   - Sends SIGINT signal to the checkpointer process (with retry logic for up to 60 seconds)
   - Optionally waits for checkpoint completion using condition variables

The function implements a sophisticated waiting mechanism when CHECKPOINT_WAIT is specified, using condition variables to detect when a new checkpoint starts and completes. It tracks checkpoint completion through counters that increment in a modulo fashion.

## Parameters / Member Variables
- `flags`: Bitwise OR of checkpoint control flags:
  - `CHECKPOINT_IS_SHUTDOWN`: Checkpoint for database shutdown
  - `CHECKPOINT_END_OF_RECOVERY`: Checkpoint for end of WAL recovery  
  - `CHECKPOINT_IMMEDIATE`: Finish ASAP, ignore checkpoint_completion_target
  - `CHECKPOINT_FORCE`: Force checkpoint even without XLOG activity
  - `CHECKPOINT_WAIT`: Wait for completion before returning
  - `CHECKPOINT_CAUSE_XLOG`: Checkpoint due to XLOG filling (affects logging)

## Dependencies
- Functions called/Symbols referenced:
  - IsPostmasterEnvironment
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [smgrdestroyall](../s/smgrdestroyall.md)
  - SpinLockAcquire/SpinLockRelease
  - kill (system call)
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - CHECK_FOR_INTERRUPTS
  - [pg_usleep](../p/pg_usleep.md)
  - elog/ereport
- Called from (representative examples):
  - [XLogWrite](../X/XLogWrite.md)
  - [StartupXLOG](../S/StartupXLOG.md)
  - [CreateDatabaseUsingFileCopy](../C/CreateDatabaseUsingFileCopy.md)
  - [dropdb](../d/dropdb.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Uses MAX_SIGNAL_TRIES (600) with 0.1 second intervals for signaling retry
- In standalone mode, always adds CHECKPOINT_IMMEDIATE flag for efficiency
- Flag values must be designed to work with bitwise OR operations for multiple requests
- Error handling differs based on CHECKPOINT_WAIT flag (ERROR vs LOG)
- Implements modulo arithmetic for checkpoint completion detection
- The function can handle checkpointer process restarts during signaling
- Checkpoint failure is detected through the ckpt_failed counter comparison

## Simplified Source

```c
// Simplified version of RequestCheckpoint
void RequestCheckpoint(int flags) {
    int ntries;
    int old_failed, old_started;

    // Handle standalone mode - do checkpoint ourselves
    if (!IsPostmasterEnvironment) {
        CreateCheckPoint(flags | CHECKPOINT_IMMEDIATE);
        smgrdestroyall();
        return;
    }

    // Set checkpoint flags atomically and capture current state
    SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
    old_failed = CheckpointerShmem->ckpt_failed;
    old_started = CheckpointerShmem->ckpt_started;
    CheckpointerShmem->ckpt_flags |= (flags | CHECKPOINT_REQUESTED);
    SpinLockRelease(&CheckpointerShmem->ckpt_lck);

    // Signal checkpointer process with retry logic
    for (ntries = 0;; ntries++) {
        if (CheckpointerShmem->checkpointer_pid == 0) {
            // Checkpointer not running
            if (ntries >= MAX_SIGNAL_TRIES || !(flags & CHECKPOINT_WAIT)) {
                elog((flags & CHECKPOINT_WAIT) ? ERROR : LOG,
                     "could not signal for checkpoint: checkpointer is not running");
                break;
            }
        } else if (kill(CheckpointerShmem->checkpointer_pid, SIGINT) != 0) {
            // Signal failed
            if (ntries >= MAX_SIGNAL_TRIES || !(flags & CHECKPOINT_WAIT)) {
                elog((flags & CHECKPOINT_WAIT) ? ERROR : LOG,
                     "could not signal for checkpoint: %m");
                break;
            }
        } else {
            break;  // Signal sent successfully
        }

        CHECK_FOR_INTERRUPTS();
        pg_usleep(100000L);  // Wait 0.1 sec before retry
    }

    // Wait for completion if requested
    if (flags & CHECKPOINT_WAIT) {
        int new_started, new_failed;

        // Wait for checkpoint to start
        ConditionVariablePrepareToSleep(&CheckpointerShmem->start_cv);
        for (;;) {
            SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
            new_started = CheckpointerShmem->ckpt_started;
            SpinLockRelease(&CheckpointerShmem->ckpt_lck);

            if (new_started != old_started)
                break;

            ConditionVariableSleep(&CheckpointerShmem->start_cv,
                                   WAIT_EVENT_CHECKPOINT_START);
        }
        ConditionVariableCancelSleep();

        // Wait for checkpoint to complete
        ConditionVariablePrepareToSleep(&CheckpointerShmem->done_cv);
        for (;;) {
            int new_done;

            SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
            new_done = CheckpointerShmem->ckpt_done;
            new_failed = CheckpointerShmem->ckpt_failed;
            SpinLockRelease(&CheckpointerShmem->ckpt_lck);

            if (new_done - new_started >= 0)
                break;

            ConditionVariableSleep(&CheckpointerShmem->done_cv,
                                   WAIT_EVENT_CHECKPOINT_DONE);
        }
        ConditionVariableCancelSleep();

        // Check for checkpoint failure
        if (new_failed != old_failed) {
            ereport(ERROR,
                    (errmsg("checkpoint request failed"),
                     errhint("Consult recent messages in the server log for details.")));
        }
    }
}
```

Key simplifications made:
- Added clear comments explaining the two execution paths (standalone vs normal)
- Simplified the signal retry loop with clearer error handling logic
- Consolidated the waiting mechanism into two clear phases (start and completion)
- Removed detailed implementation comments to focus on the core algorithm
- Maintained all essential synchronization and error handling logic
- Preserved the modulo arithmetic for checkpoint completion detection