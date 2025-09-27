# CheckpointerMain

## Location
[src/backend/postmaster/checkpointer.c:176-560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/checkpointer.c#L176-L560)

## Overview
Main entry point for the checkpointer process that performs database checkpoints and manages WAL archiving in PostgreSQL.

## Definition

```c
struct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 *
	 * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
	 * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
	 * signals other than SIGQUIT will be blocked until we complete error
	 * recovery.  It might seem that this policy makes the HOLD_INTERRUPTS()
	 * call redundant, but it is not since InterruptPending might be set
	 * already.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in checkpointer, but we do have LWLocks, buffers, and temp
		 * files.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgstat_report_wait_end();
		UnlockBuffers();
		ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/* Warn any waiting backends that the checkpoint failed. */
		if (ckpt_active)
		{
			SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
			CheckpointerShmem->ckpt_failed++;
			CheckpointerShmem->ckpt_done = CheckpointerShmem->ckpt_started;
			SpinLockRelease(&CheckpointerShmem->ckpt_lck);

			ConditionVariableBroadcast(&CheckpointerShmem->done_cv);

			ckpt_active = false;
		}

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(checkpointer_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextReset(checkpointer_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;
```
## Detailed Description
CheckpointerMain is the core function of the checkpointer background process, responsible for:
- Performing periodic checkpoints to ensure database consistency
- Managing WAL (Write-Ahead Log) archiving timeouts  
- Handling checkpoint requests from other processes
- Managing memory contexts and error recovery for the checkpointer process
- Coordinating with other backend processes through shared memory

The function runs in an infinite loop, sleeping between checkpoint operations and responding to signals for checkpoint requests. It handles both time-driven checkpoints (based on CheckPointTimeout) and request-driven checkpoints from other processes. During recovery, it performs restartpoints instead of full checkpoints.

The process sets up signal handlers for configuration reloads, checkpoint requests, and shutdown signals. It uses a dedicated memory context for error recovery and implements comprehensive cleanup procedures when errors occur.

## Parameters
- `startup_data`: Startup data passed from the postmaster (expected to be NULL/empty)
- `startup_data_len`: Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - [HandleCheckpointerInterrupts](../H/HandleCheckpointerInterrupts.md)  
  - [CheckArchiveTimeout](CheckArchiveTimeout.md)
  - [CreateCheckPoint](CreateCheckPoint.md)
  - [CreateRestartPoint](CreateRestartPoint.md)
  - [AbsorbSyncRequests](../A/AbsorbSyncRequests.md)
  - [UpdateSharedMemoryConfig](../U/UpdateSharedMemoryConfig.md)
  - [ResetLatch](../R/ResetLatch.md)/WaitLatch
  - [pgstat_report_checkpointer](../p/pgstat_report_checkpointer.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetInsertRecPtr](../G/GetInsertRecPtr.md)/GetXLogReplayRecPtr
- Called from (representative examples):
  - child_process_kind (launch_backend.c:204)

## Notes and Other Information
- Ignores SIGTERM to avoid premature shutdown during system-wide shutdowns
- Uses sigsetjmp/longjmp for error recovery instead of PG_TRY/PG_CATCH
- Maintains separate statistics for checkpoints vs restartpoints
- Implements checkpoint frequency warnings when checkpoints occur too frequently
- Coordinates with waiting backends through condition variables in shared memory
- Performs cleanup of storage manager objects after each checkpoint to handle dropped relations

## Simplified Source

```c
// Simplified version of CheckpointerMain
void CheckpointerMain(char *startup_data, size_t startup_data_len) {
    sigjmp_buf local_sigjmp_buf;
    MemoryContext checkpointer_context;

    // Step 1: Initialize process type and shared memory
    MyBackendType = B_CHECKPOINTER;
    AuxiliaryProcessMainCommon();
    CheckpointerShmem->checkpointer_pid = MyProcPid;

    // Step 2: Set up signal handlers
    pqsignal(SIGHUP, SignalHandlerForConfigReload);    // config reload
    pqsignal(SIGINT, ReqCheckpointHandler);            // checkpoint request
    pqsignal(SIGTERM, SIG_IGN);                        // ignore shutdown
    pqsignal(SIGUSR2, SignalHandlerForShutdownRequest); // graceful shutdown

    // Step 3: Initialize timing variables
    last_checkpoint_time = last_xlog_switch_time = (pg_time_t) time(NULL);

    // Step 4: Create dedicated memory context for error recovery
    checkpointer_context = AllocSetContextCreate(TopMemoryContext,
                                                "Checkpointer",
                                                ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(checkpointer_context);

    // Step 5: Error recovery setup using setjmp
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        // Error recovery: clean up resources and reset state
        error_context_stack = NULL;
        HOLD_INTERRUPTS();
        EmitErrorReport();

        // Release locks and clean up resources
        LWLockReleaseAll();
        UnlockBuffers();
        ReleaseAuxProcessResources(false);

        // Notify waiting backends of checkpoint failure
        if (ckpt_active) {
            SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
            CheckpointerShmem->ckpt_failed++;
            CheckpointerShmem->ckpt_done = CheckpointerShmem->ckpt_started;
            SpinLockRelease(&CheckpointerShmem->ckpt_lck);
            ConditionVariableBroadcast(&CheckpointerShmem->done_cv);
            ckpt_active = false;
        }

        // Reset memory context and resume operations
        MemoryContextSwitchTo(checkpointer_context);
        FlushErrorState();
        MemoryContextReset(checkpointer_context);
        RESUME_INTERRUPTS();

        // Wait before retrying to avoid log spam
        pg_usleep(1000000L);
    }

    // Step 6: Enable exception handling and unblock signals
    PG_exception_stack = &local_sigjmp_buf;
    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);
    UpdateSharedMemoryConfig();
    ProcGlobal->checkpointerLatch = &MyProc->procLatch;

    // Step 7: Main processing loop
    for (;;) {
        bool do_checkpoint = false;
        int flags = 0;
        pg_time_t now;
        int elapsed_secs;

        // Clear pending wakeups and process requests
        ResetLatch(MyLatch);
        AbsorbSyncRequests();
        HandleCheckpointerInterrupts();

        // Check for explicit checkpoint requests
        if (CheckpointerShmem->ckpt_flags) {
            do_checkpoint = true;
        }

        // Check for time-based checkpoint requirement
        now = (pg_time_t) time(NULL);
        elapsed_secs = now - last_checkpoint_time;
        if (elapsed_secs >= CheckPointTimeout) {
            do_checkpoint = true;
            flags |= CHECKPOINT_CAUSE_TIME;
        }

        // Perform checkpoint if needed
        if (do_checkpoint) {
            bool checkpoint_performed = false;
            bool do_restartpoint = RecoveryInProgress();

            // Acquire checkpoint flags atomically
            SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
            flags |= CheckpointerShmem->ckpt_flags;
            CheckpointerShmem->ckpt_flags = 0;
            CheckpointerShmem->ckpt_started++;
            SpinLockRelease(&CheckpointerShmem->ckpt_lck);

            // Initialize checkpoint state
            ckpt_active = true;
            ckpt_start_time = now;

            // Perform either checkpoint or restartpoint
            if (!do_restartpoint) {
                CreateCheckPoint(flags);
                checkpoint_performed = true;
            } else {
                checkpoint_performed = CreateRestartPoint(flags);
            }

            // Clean up and notify completion
            smgrdestroyall();  // Free storage manager objects

            SpinLockAcquire(&CheckpointerShmem->ckpt_lck);
            CheckpointerShmem->ckpt_done = CheckpointerShmem->ckpt_started;
            SpinLockRelease(&CheckpointerShmem->ckpt_lck);
            ConditionVariableBroadcast(&CheckpointerShmem->done_cv);

            // Update timing for next checkpoint
            if (checkpoint_performed) {
                last_checkpoint_time = now;
            } else {
                // Retry failed restartpoint in 15 seconds
                last_checkpoint_time = now - CheckPointTimeout + 15;
            }

            ckpt_active = false;
        }

        // Handle WAL archiving timeout
        CheckArchiveTimeout();

        // Report statistics
        pgstat_report_checkpointer();
        pgstat_report_wal(true);

        // Skip sleep if new checkpoint requests arrived
        if (CheckpointerShmem->ckpt_flags) {
            continue;
        }

        // Calculate sleep time until next checkpoint or archive timeout
        now = (pg_time_t) time(NULL);
        elapsed_secs = now - last_checkpoint_time;
        if (elapsed_secs >= CheckPointTimeout) {
            continue;  // Time for immediate checkpoint
        }

        int timeout = CheckPointTimeout - elapsed_secs;
        if (XLogArchiveTimeout > 0 && !RecoveryInProgress()) {
            elapsed_secs = now - last_xlog_switch_time;
            if (elapsed_secs >= XLogArchiveTimeout) {
                continue;  // Time for WAL switch
            }
            timeout = Min(timeout, XLogArchiveTimeout - elapsed_secs);
        }

        // Sleep until next event
        WaitLatch(MyLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                  timeout * 1000L,
                  WAIT_EVENT_CHECKPOINTER_MAIN);
    }
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Consolidated signal handler setup into essential handlers only
- Abstracted complex checkpoint coordination logic
- Simplified memory context management
- Removed verbose warning logic for frequent checkpoints
- Focused on the main execution path (checkpoint vs restartpoint decision)
- Consolidated statistics tracking and timing calculations
- Streamlined the sleep/wake cycle logic