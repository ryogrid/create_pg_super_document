# AtEOSubXact_PgStat_Relations

## Location
[src/backend/utils/activity/pgstat_relation.c:595-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L595-L675)

## Overview
Performs relation-specific statistics cleanup and consolidation at the end of a subtransaction, transferring counters to the parent transaction level.

## Definition

```c
void
AtEOSubXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth)
```
## Detailed Description
This function is a helper for AtEOSubXact_PgStat that handles relation-specific end-of-subtransaction work. It processes table transaction status entries from the completed subtransaction and handles them based on whether the subtransaction is committing or aborting:

For commits: Transfers insert/update/delete counts to the parent transaction level. If the subtransaction involved truncate/drop operations, it propagates those status changes upward. When no immediate parent exists, it relinks the transaction record into the appropriate parent level.

For aborts: Applies the attempted actions to the top-level statistics as dead tuples, restores any counters that were affected by truncate/drop operations, and discards the subtransaction state.

## Parameters / Member Variables
- `*xact_state`: Subtransaction status containing the subtransaction's relation statistics
- `isCommit`: Boolean indicating whether the subtransaction is committing (true) or aborting (false)
- `nestDepth`: The nesting level of the subtransaction being processed
## Dependencies
- Functions called/Symbols referenced:
  - [save_truncdrop_counters](../s/save_truncdrop_counters.md) (saves counters before truncate/drop operations)
  - [restore_truncdrop_counters](../r/restore_truncdrop_counters.md) (restores counters after aborted truncate/drop)
  - [pgstat_get_xact_stack_level](../p/pgstat_get_xact_stack_level.md) (gets transaction stack level for relinking)
  - [pfree](../p/pfree.md) (memory deallocation)
  - [PgStat_SubXactStatus](../P/PgStat_SubXactStatus.md) (subtransaction status structure)
  - [PgStat_TableXactStatus](../P/PgStat_TableXactStatus.md) (transaction-level table statistics)
  - [PgStat_TableStatus](../P/PgStat_TableStatus.md) (base table statistics structure)
- Called from (representative examples):
  - [AtEOSubXact_PgStat](AtEOSubXact_PgStat.md) (main end-of-subtransaction statistics handler)

## Notes and Other Information
- Handles complex transaction nesting scenarios with proper counter propagation
- For commits with immediate parent: Adds counters to parent or replaces them if truncate/drop occurred
- For commits without immediate parent: Relinks transaction record to appropriate nesting level
- For aborts: Always updates top-level counts and treats attempted inserts/updates as dead tuples
- Uses Assert statements to validate transaction nesting level consistency
- Memory is managed within TopTransactionContext for automatic cleanup
- Truncate/drop operations require special handling to maintain statistics consistency across transaction boundaries

## Simplified Source

```c
void
AtEOSubXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth)
{
    PgStat_TableXactStatus *trans;
    PgStat_TableXactStatus *next_trans;

    // Process each table transaction in this subtransaction
    for (trans = xact_state->first; trans != NULL; trans = next_trans)
    {
        PgStat_TableStatus *tabstat;

        next_trans = trans->next;
        Assert(trans->nest_level == nestDepth);
        tabstat = trans->parent;
        Assert(tabstat->trans == trans);

        if (isCommit)
        {
            // Subtransaction commit: propagate counts upward
            if (trans->upper && trans->upper->nest_level == nestDepth - 1)
            {
                if (trans->truncdropped)
                {
                    // Propagate truncate/drop status and replace counters
                    save_truncdrop_counters(trans->upper, false);
                    trans->upper->tuples_inserted = trans->tuples_inserted;
                    trans->upper->tuples_updated = trans->tuples_updated;
                    trans->upper->tuples_deleted = trans->tuples_deleted;
                }
                else
                {
                    // Accumulate counters
                    trans->upper->tuples_inserted += trans->tuples_inserted;
                    trans->upper->tuples_updated += trans->tuples_updated;
                    trans->upper->tuples_deleted += trans->tuples_deleted;
                }
                tabstat->trans = trans->upper;
                pfree(trans);
            }
            else
            {
                // No immediate parent: relink to appropriate level
                PgStat_SubXactStatus *upper_xact_state;

                upper_xact_state = pgstat_get_xact_stack_level(nestDepth - 1);
                trans->next = upper_xact_state->first;
                upper_xact_state->first = trans;
                trans->nest_level = nestDepth - 1;
            }
        }
        else
        {
            // Subtransaction abort: update top-level counts
            restore_truncdrop_counters(trans);

            // Count attempted actions regardless of commit/abort
            tabstat->counts.tuples_inserted += trans->tuples_inserted;
            tabstat->counts.tuples_updated += trans->tuples_updated;
            tabstat->counts.tuples_deleted += trans->tuples_deleted;

            // Inserted/updated tuples become dead
            tabstat->counts.delta_dead_tuples +=
                trans->tuples_inserted + trans->tuples_updated;

            tabstat->trans = trans->upper;
            pfree(trans);
        }
    }
}
```