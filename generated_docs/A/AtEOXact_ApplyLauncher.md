# AtEOXact_ApplyLauncher

## Location
[src/backend/replication/logical/launcher.c:1099-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L1099-L1117)

## Overview
Transaction end callback that conditionally wakes up the logical replication launcher when a transaction commits, ensuring that subscription changes take effect promptly.

## Definition
```c
void AtEOXact_ApplyLauncher(bool isCommit)
```

## Detailed Description
This function is a transaction callback (At End Of Transaction) that handles launcher wake-up requests that were deferred during transaction processing. It's called by PostgreSQL's transaction management system at the end of every transaction to allow the logical replication launcher to respond to changes that occurred during the transaction.

The function follows a common PostgreSQL pattern for transaction callbacks: it only performs the requested action (waking the launcher) if the transaction committed successfully. If the transaction aborted, no action is taken since any changes made during the transaction were rolled back. Regardless of the transaction outcome, the wake-up request flag is reset to false.

This mechanism is essential for subscription management operations that need to notify the launcher about changes (like subscription creation, modification, or deletion) but only after the changes have been committed successfully.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether the transaction committed (true) or aborted (false)

## Dependencies
- Functions called/Symbols referenced:
  - ApplyLauncherWakeup
- Called from:
  - CommitTransaction
  - PrepareTransaction  
  - AbortTransaction

## Notes and Other Information
- This is a public function exported in logicallauncher.h as part of the transaction callback interface
- Works in conjunction with the global variable `on_commit_launcher_wakeup` to track pending wake-up requests
- The flag is reset regardless of commit/abort status to ensure clean state for the next transaction
- Part of PostgreSQL's standard transaction callback mechanism, following the "AtEOXact_" naming convention
- Ensures that launcher wake-ups only happen for successfully committed changes, maintaining data consistency
- Called automatically by the transaction system - not typically invoked directly by application code