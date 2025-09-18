# AtEOXact_PgStat_Relations

## Location
src/backend/utils/activity/pgstat_relation.c: 537 - 594

## Overview
Performs relation-specific statistics cleanup and consolidation at the end of a transaction, transferring transactional counters into base statistics entries.

## Definition


## Detailed Description
This function is a helper for AtEOXact_PgStat that handles relation-specific end-of-transaction work. It processes all table transaction status entries from the completed transaction and transfers their insert/update/delete counts into the corresponding base table statistics entries. The function handles both commit and abort scenarios differently:

For commits: Applies all changes including live/dead tuple deltas, truncation status, and change events.
For aborts: Restores pre-truncate/drop statistics, counts attempted actions as dead tuples, but doesn't generate change events.

The function doesn't free transactional state memory since it resides in TopTransactionContext and will be automatically cleaned up.

## Parameters / Member Variables
- : Subtransaction status containing the transaction's relation statistics
- : Boolean indicating whether the transaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - restore_truncdrop_counters (restores counters after aborted truncate/drop)
  - PgStat_SubXactStatus (subtransaction status structure)
  - PgStat_TableXactStatus (transaction-level table statistics)
  - PgStat_TableStatus (base table statistics structure)
- Called from (representative examples):
  - AtEOXact_PgStat (main end-of-transaction statistics handler)

## Notes and Other Information
- Only processes top-level transactions (nest_level == 1)
- Tuple count changes are always applied regardless of commit/abort status
- Live/dead tuple deltas and change events are only updated on commit
- Truncation/drop status is preserved on commit but reverted on abort
- The trans pointer in each table status entry is reset to NULL after processing
- Memory cleanup is handled automatically via TopTransactionContext
- Uses Assert statements to validate transaction nesting structure assumptions