# AtPrepare_PgStat_Relations

## Location
[src/backend/utils/activity/pgstat_relation.c:676-713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L676-L713)

## Overview
Generates 2PC (Two-Phase Commit) records for all pending transaction-dependent relation statistics during prepared transaction processing.

## Definition

```c
void
AtPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
```
## Detailed Description
This function is called during the prepare phase of a two-phase commit transaction to create persistent records of all relation statistics that have been accumulated during the transaction. It iterates through all table transaction status entries and creates TwoPhasePgStatRecord structures containing the statistics data, which are then registered with the two-phase commit system via RegisterTwoPhaseRecord.

The function ensures that relation statistics are preserved across the prepare/commit phases of distributed transactions, allowing the statistics to be properly applied or discarded based on the final transaction outcome. Each record contains tuple counts (inserted, updated, deleted), pre-truncate/drop statistics, and truncation status.

## Parameters / Member Variables
- `*xact_state`: Subtransaction status containing all relation statistics for the transaction
## Dependencies
- Functions called/Symbols referenced:
  - [RegisterTwoPhaseRecord](../R/RegisterTwoPhaseRecord.md) (registers 2PC record with the system)
  - [TwoPhasePgStatRecord](../T/TwoPhasePgStatRecord.md) (structure for 2PC statistics record)
  - TWOPHASE_RM_PGSTAT_ID (resource manager ID for statistics)
  - [PgStat_SubXactStatus](../P/PgStat_SubXactStatus.md) (subtransaction status structure)
  - [PgStat_TableXactStatus](../P/PgStat_TableXactStatus.md) (transaction-level table statistics)
  - [PgStat_TableStatus](../P/PgStat_TableStatus.md) (base table statistics structure)
  - PG_USED_FOR_ASSERTS_ONLY (annotation for debug-only variables)
- Called from (representative examples):
  - [AtPrepare_PgStat](AtPrepare_PgStat.md) (main prepare phase statistics handler)

## Notes and Other Information
- Only processes top-level transactions (nest_level == 1) during prepare phase
- Creates persistent 2PC records that survive server restarts during prepared transaction state
- Records include both current transaction counters and pre-truncate/drop backup counters
- Uses Assert statements to validate transaction structure assumptions
- Each record is registered with a fixed size of sizeof(TwoPhasePgStatRecord)
- The statistics data will be applied or discarded during the subsequent commit/abort phase
- Essential for maintaining statistics consistency in distributed transaction scenarios
- Handles both regular tables and shared system tables via the shared flag

## Simplified Source

```c
void
AtPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
{
    PgStat_TableXactStatus *trans;

    // Iterate through all table transaction status entries
    for (trans = xact_state->first; trans != NULL; trans = trans->next)
    {
        TwoPhasePgStatRecord record;

        // Copy transaction statistics to 2PC record
        record.tuples_inserted = trans->tuples_inserted;
        record.tuples_updated = trans->tuples_updated;
        record.tuples_deleted = trans->tuples_deleted;
        record.inserted_pre_truncdrop = trans->inserted_pre_truncdrop;
        record.updated_pre_truncdrop = trans->updated_pre_truncdrop;
        record.deleted_pre_truncdrop = trans->deleted_pre_truncdrop;
        record.id = trans->parent->id;
        record.shared = trans->parent->shared;
        record.truncdropped = trans->truncdropped;

        // Register the record with the two-phase commit system
        RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0,
                              &record, sizeof(TwoPhasePgStatRecord));
    }
}
```