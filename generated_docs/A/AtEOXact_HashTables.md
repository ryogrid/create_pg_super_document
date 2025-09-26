# AtEOXact_HashTables

## Location
[src/backend/utils/hash/dynahash.c:1872-1897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1872-L1897)

## Overview
Cleans up any remaining active sequential scan operations at the end of a transaction, handling both commit and abort scenarios.

## Definition
```c
void AtEOXact_HashTables(bool isCommit)
```

## Detailed Description
This function is called at the end of every transaction (both commit and abort) to clean up any remaining active hash table sequential scans. During transaction abort, it silently cleans up any open scans since they are expected during cleanup. However, during transaction commit, it issues warnings for any remaining active scans, as this indicates a programming error where `hash_seq_term()` was not properly called. The function resets the global sequential scan counter to zero, effectively clearing all tracked scans. This is part of PostgreSQL's transaction cleanup infrastructure that ensures resources are properly released.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the transaction is being committed (true) or aborted (false). This determines whether warnings should be issued for remaining active scans.

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only global variables and elog)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [PrepareTransaction](../P/PrepareTransaction.md)
  - [AbortTransaction](AbortTransaction.md)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [pgarch_archiveXlog](../p/pgarch_archiveXlog.md)
  - [WalSummarizerMain](../W/WalSummarizerMain.md)
  - [WalWriterMain](../W/WalWriterMain.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)

## Notes and Other Information
- This is a public function, callable from outside dynahash.c
- Part of PostgreSQL's "AtEOXact" (At End Of Transaction) cleanup infrastructure
- Deliberately avoids printing table names to prevent accessing potentially deallocated memory
- Called by various background processes to ensure clean state
- The function provides different behavior for commit vs. abort scenarios to aid in debugging
- Critical for preventing resource leaks and maintaining system stability across transaction boundaries
- Works as the cleanup counterpart to the register/deregister/has_seq_scans family of functions