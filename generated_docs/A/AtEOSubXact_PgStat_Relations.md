# AtEOSubXact_PgStat_Relations

## Location
src/backend/utils/activity/pgstat_relation.c: 595 - 675

## Overview
Performs relation-specific statistics cleanup and consolidation at the end of a subtransaction, transferring counters to the parent transaction level.

## Definition


## Detailed Description
This function is a helper for AtEOSubXact_PgStat that handles relation-specific end-of-subtransaction work. It processes table transaction status entries from the completed subtransaction and handles them based on whether the subtransaction is committing or aborting:

For commits: Transfers insert/update/delete counts to the parent transaction level. If the subtransaction involved truncate/drop operations, it propagates those status changes upward. When no immediate parent exists, it relinks the transaction record into the appropriate parent level.

For aborts: Applies the attempted actions to the top-level statistics as dead tuples, restores any counters that were affected by truncate/drop operations, and discards the subtransaction state.

## Parameters / Member Variables
- : Subtransaction status containing the subtransaction's relation statistics
- : Boolean indicating whether the subtransaction is committing (true) or aborting (false) 
- : The nesting level of the subtransaction being processed

## Dependencies
- Functions called/Symbols referenced:
  - save_truncdrop_counters (saves counters before truncate/drop operations)
  - restore_truncdrop_counters (restores counters after aborted truncate/drop)
  - pgstat_get_xact_stack_level (gets transaction stack level for relinking)
  - pfree (memory deallocation)
  - PgStat_SubXactStatus (subtransaction status structure)
  - PgStat_TableXactStatus (transaction-level table statistics)
  - PgStat_TableStatus (base table statistics structure)
- Called from (representative examples):
  - AtEOSubXact_PgStat (main end-of-subtransaction statistics handler)

## Notes and Other Information
- Handles complex transaction nesting scenarios with proper counter propagation
- For commits with immediate parent: Adds counters to parent or replaces them if truncate/drop occurred
- For commits without immediate parent: Relinks transaction record to appropriate nesting level
- For aborts: Always updates top-level counts and treats attempted inserts/updates as dead tuples
- Uses Assert statements to validate transaction nesting level consistency
- Memory is managed within TopTransactionContext for automatic cleanup
- Truncate/drop operations require special handling to maintain statistics consistency across transaction boundaries