# WalWriterMain

## Location
[src/backend/postmaster/walwriter.c:89-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walwriter.c#L89-L273)

## Overview
WalWriterMain is the main entry point for the WAL (Write-Ahead Logging) writer background process, responsible for continuously flushing WAL data to disk to ensure data durability and performance.

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
		 * about in walwriter, but we do have LWLocks, and perhaps buffers?
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

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(walwriter_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextReset(walwriter_context);

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
The WAL writer is a background process that periodically flushes Write-Ahead Log data to storage. This function implements the main loop of the WAL writer process, which:

1. Sets up signal handlers for proper process management and shutdown
2. Creates a dedicated memory context to avoid memory leaks during error recovery
3. Implements comprehensive error recovery using sigsetjmp/siglongjmp
4. Runs an infinite loop that:
   - Flushes WAL data using XLogBackgroundFlush()
   - Implements hibernation to reduce power consumption when idle
   - Reports WAL statistics to the cumulative stats system
   - Sleeps until signaled or timeout expires

The process uses a hibernation mechanism to reduce CPU usage when there's little WAL activity - after several cycles without useful work, it extends its sleep time by HIBERNATE_FACTOR.

## Parameters

- `startup_data`: Startup data passed from the postmaster (unused, expected to be NULL)
- `startup_data_len`: Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - [XLogBackgroundFlush](../X/XLogBackgroundFlush.md)
  - [SetWalWriterSleeping](../S/SetWalWriterSleeping.md)
  - [pgstat_report_wal](../p/pgstat_report_wal.md)
  - [HandleMainLoopInterrupts](../H/HandleMainLoopInterrupts.md)
  - [WaitLatch](WaitLatch.md)
  - [pqsignal](../p/pqsignal.md) (for signal handler setup)
  - AllocSetContextCreate
  - [EmitErrorReport](../E/EmitErrorReport.md)
  - [LWLockReleaseAll](../L/LWLockReleaseAll.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - [UnlockBuffers](../U/UnlockBuffers.md)
  - [ReleaseAuxProcessResources](../R/ReleaseAuxProcessResources.md)
  - Various AtEOXact_* functions for cleanup
- Called from (representative examples):
  - child_process_kind (in launch_backend.c)

## Notes and Other Information
- The function never returns under normal circumstances - it runs an infinite loop
- Uses sigsetjmp for error recovery instead of PG_TRY to maintain an exception handler during error recovery
- Implements a sophisticated hibernation mechanism using LOOPS_UNTIL_HIBERNATE and HIBERNATE_FACTOR constants
- Sets MyBackendType to B_WAL_WRITER to identify the process type
- Creates and advertises a latch (ProcGlobal->walwriterLatch) that other processes can use to wake it up
- Sleeps for at least 1 second after any error to prevent rapid error log filling
- Uses WalWriterDelay configuration parameter to control sleep intervals
- The process can be signaled via SIGHUP (config reload), SIGTERM/SIGINT (shutdown), and SIGUSR1 (inter-process signaling)