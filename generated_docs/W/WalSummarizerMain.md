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