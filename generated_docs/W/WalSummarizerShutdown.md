# WalSummarizerShutdown

## Location
[src/backend/postmaster/walsummarizer.c:788-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L788-L799)

## Overview
A cleanup function that marks the WAL summarizer process as no longer running in shared memory during process termination.

## Definition

```c
static void
WalSummarizerShutdown(int code, Datum arg)
```
## Detailed Description
This is a static cleanup function designed to be registered as an exit callback for the WAL summarizer process. When the summarizer process terminates (whether normally or abnormally), this function ensures that the shared memory control structure is updated to reflect that the summarizer is no longer running. This prevents other processes from attempting to communicate with a non-existent summarizer process.

The function uses an exclusive lock to safely update the shared memory structure, setting the summarizer's process number to an invalid value to indicate that no summarizer process is currently active.

## Parameters / Member Variables
- : Exit code (standard exit callback parameter, not used in this function)
- : Additional argument (standard exit callback parameter, not used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - INVALID_PROC_NUMBER (constant)
- Global variables accessed:
  - WalSummarizerCtl
- Called from (representative examples):
  - [WalSummarizerMain](WalSummarizerMain.md) (registered as exit callback)

## Notes and Other Information
- This is a static function, only accessible within the walsummarizer.c file
- Designed to be registered with on_proc_exit() or similar mechanism
- Uses exclusive locking to ensure atomic update of the shared state
- Critical for maintaining consistency of the WAL summarizer's process tracking
- The function parameters follow the standard PostgreSQL exit callback signature but are not used in the implementation