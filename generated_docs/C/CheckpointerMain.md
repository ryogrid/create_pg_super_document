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