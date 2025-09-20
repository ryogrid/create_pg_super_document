# GetWalSummarizerState

## Location
[src/backend/postmaster/walsummarizer.c:447-504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L447-L504)

## Overview
Retrieves the current state information of the WAL summarizer from shared memory, including progress and process details.

## Definition

```c
structure contents are
		 * undefined.
		 */
		*summarized_tli = 0;
```
## Detailed Description
This function provides a thread-safe way to query the current state of the WAL summarizer subsystem. It acquires a shared lock on the WAL summarizer control structure and returns information about the summarization progress. The function handles both initialized and uninitialized states, returning appropriate default values when the summarizer hasn't been initialized yet. It also correctly handles cases where the summarizer process has exited.

## Parameters / Member Variables
- : Output parameter for the timeline ID up to which WAL has been summarized
- : Output parameter for the LSN up to which WAL has been fully summarized
- : Output parameter for the LSN up to which WAL has been read (may be ahead of summarized_lsn)
- : Output parameter for the process ID of the running summarizer (-1 if not running)

## Dependencies
- Functions called/Symbols referenced:
  - LW_SHARED (lock mode constant)
  - INVALID_PROC_NUMBER (constant for invalid process number)
  - GetPGProcByNumber (converts process number to process structure)
- Called from (representative examples):
  - [pg_get_wal_summarizer_state](../p/pg_get_wal_summarizer_state.md) (in src/backend/backup/walsummaryfuncs.c:188)

## Notes and Other Information
- Uses shared locking to allow concurrent reads of the state
- Returns safe default values (-1 for PID, 0 for TLI, InvalidXLogRecPtr for LSNs) when uninitialized
- Handles process exit detection by checking for INVALID_PROC_NUMBER
- Normalizes invalid PID values (≤0) to -1 for consistent API
- The function provides a snapshot view - values may become stale immediately after return
- Location: src/backend/postmaster/walsummarizer.c:447-504