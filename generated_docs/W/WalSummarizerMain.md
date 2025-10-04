# WalSummarizerMain

## Location
[src/backend/postmaster/walsummarizer.c:211-446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L211-L446)

## Overview
The main entry point function for the WAL summarizer background process that continuously creates summary files from WAL records.

## Definition

```c
structure contents are
		 * undefined.
		 */
		*summarized_tli = 0;
```
## Detailed Description
WalSummarizerMain is the core function that implements the WAL summarizer background process. It sets up signal handlers, initializes memory contexts, and runs an infinite loop that continuously reads WAL records and creates summary files. The function handles timeline switches, manages progress tracking in shared memory, and coordinates with other processes through condition variables. It implements error recovery with a 10-second retry delay and maintains state about the current summarization position (LSN and timeline).

Key responsibilities include:
- Setting up signal handlers for process management
- Managing memory contexts for error recovery
- Tracking current position (LSN, timeline) for summarization
- Handling timeline switches in standby scenarios
- Coordinating with other processes via shared memory
- Creating WAL summary files continuously
- Implementing error recovery and retry logic

## Parameters / Member Variables
- : Startup data passed to the process (currently unused, expected to be NULL)
- : Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md) (standard auxiliary process setup)
  - [GetOldestUnsummarizedLSN](../G/GetOldestUnsummarizedLSN.md) (determines starting position)
  - [GetLatestLSN](../G/GetLatestLSN.md) (finds latest available WAL position)
  - [SummarizeWAL](../S/SummarizeWAL.md) (core WAL summarization logic)
  - [HandleWalSummarizerInterrupts](../H/HandleWalSummarizerInterrupts.md) (signal processing)
  - [MaybeRemoveOldWalSummaries](../M/MaybeRemoveOldWalSummaries.md) (cleanup old files)
  - [WalSummarizerShutdown](WalSummarizerShutdown.md) (cleanup on exit)
  - Various PostgreSQL utility functions for memory, locking, signals
- Called from (representative examples):
  - child_process_kind (in src/backend/postmaster/launch_backend.c:207)

## Notes and Other Information
- Runs as B_WAL_SUMMARIZER background process type
- Uses WALSummarizerLock for shared memory coordination
- Implements 10-second retry delay on errors to avoid log spam
- Handles timeline switches for standby servers
- Updates shared memory state after each summarization cycle
- Uses condition variables to wake up waiting processes
- Location: src/backend/postmaster/walsummarizer.c:211-446

## Simplified Source

```c
void WalSummarizerMain(char *startup_data, size_t startup_data_len)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext context;
    XLogRecPtr current_lsn, switch_lsn = InvalidXLogRecPtr;
    TimeLineID current_tli, switch_tli = 0;
    bool exact;

    Assert(startup_data_len == 0);

    // Initialize as WAL summarizer background process
    MyBackendType = B_WAL_SUMMARIZER;
    AuxiliaryProcessMainCommon();

    // Set up signal handlers for process management
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGINT, SignalHandlerForShutdownRequest);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);

    // Register for cleanup and advertise in shared memory
    on_shmem_exit(WalSummarizerShutdown, (Datum) 0);
    LWLockAcquire(WALSummarizerLock, LW_EXCLUSIVE);
    WalSummarizerCtl->summarizer_pgprocno = MyProcNumber;
    LWLockRelease(WALSummarizerLock);

    // Create memory context for error recovery
    context = AllocSetContextCreate(TopMemoryContext, "Wal Summarizer",
                                   ALLOCSET_DEFAULT_SIZES);

    // Set up error recovery point
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        // Error recovery: cleanup resources and retry after delay
        EmitErrorReport();
        LWLockReleaseAll();
        ReleaseAuxProcessResources(false);
        MemoryContextReset(context);

        // Wait 10 seconds before retry to avoid log spam
        WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10000,
                 WAIT_EVENT_WAL_SUMMARIZER_ERROR);
    }

    PG_exception_stack = &local_sigjmp_buf;
    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

    // Get starting position for summarization
    current_lsn = GetOldestUnsummarizedLSN(&current_tli, &exact);
    if (XLogRecPtrIsInvalid(current_lsn))
        proc_exit(0);

    // Main summarization loop
    for (;;) {
        XLogRecPtr latest_lsn, end_of_summary_lsn;
        TimeLineID latest_tli;

        MemoryContextReset(context);
        HandleWalSummarizerInterrupts();
        MaybeRemoveOldWalSummaries();

        // Find latest available WAL position
        latest_lsn = GetLatestLSN(&latest_tli);

        // Handle timeline switches for standby scenarios
        if (current_tli != latest_tli && XLogRecPtrIsInvalid(switch_lsn)) {
            List *tles = readTimeLineHistory(latest_tli);
            switch_lsn = tliSwitchPoint(current_tli, tles, &switch_tli);
        }

        // Switch to next timeline if needed
        if (!XLogRecPtrIsInvalid(switch_lsn) && current_lsn >= switch_lsn) {
            current_tli = switch_tli;
            current_lsn = switch_lsn;
            switch_lsn = InvalidXLogRecPtr;
            switch_tli = 0;

            // Update shared memory state
            LWLockAcquire(WALSummarizerLock, LW_EXCLUSIVE);
            WalSummarizerCtl->summarized_lsn = current_lsn;
            WalSummarizerCtl->summarized_tli = current_tli;
            WalSummarizerCtl->lsn_is_exact = true;
            WalSummarizerCtl->pending_lsn = current_lsn;
            LWLockRelease(WALSummarizerLock);
            continue;
        }

        // Core summarization work
        end_of_summary_lsn = SummarizeWAL(current_tli, current_lsn, exact,
                                         switch_lsn, latest_lsn);

        // Update state for next iteration
        current_lsn = end_of_summary_lsn;
        exact = true;

        // Update shared memory and wake waiters
        LWLockAcquire(WALSummarizerLock, LW_EXCLUSIVE);
        WalSummarizerCtl->summarized_lsn = end_of_summary_lsn;
        WalSummarizerCtl->summarized_tli = current_tli;
        WalSummarizerCtl->lsn_is_exact = true;
        WalSummarizerCtl->pending_lsn = end_of_summary_lsn;
        LWLockRelease(WALSummarizerLock);

        ConditionVariableBroadcast(&WalSummarizerCtl->summary_file_cv);
    }
}
```