# AtPrepare_PgStat_Relations

## Location
src/backend/utils/activity/pgstat_relation.c: 676 - 713

## Overview
Generates 2PC (Two-Phase Commit) records for all pending transaction-dependent relation statistics during prepared transaction processing.

## Definition


## Detailed Description
This function is called during the prepare phase of a two-phase commit transaction to create persistent records of all relation statistics that have been accumulated during the transaction. It iterates through all table transaction status entries and creates TwoPhasePgStatRecord structures containing the statistics data, which are then registered with the two-phase commit system via RegisterTwoPhaseRecord.

The function ensures that relation statistics are preserved across the prepare/commit phases of distributed transactions, allowing the statistics to be properly applied or discarded based on the final transaction outcome. Each record contains tuple counts (inserted, updated, deleted), pre-truncate/drop statistics, and truncation status.

## Parameters / Member Variables
- : Subtransaction status containing all relation statistics for the transaction

## Dependencies
- Functions called/Symbols referenced:
  - RegisterTwoPhaseRecord (registers 2PC record with the system)
  - TwoPhasePgStatRecord (structure for 2PC statistics record)
  - TWOPHASE_RM_PGSTAT_ID (resource manager ID for statistics)
  - PgStat_SubXactStatus (subtransaction status structure)
  - PgStat_TableXactStatus (transaction-level table statistics)
  - PgStat_TableStatus (base table statistics structure)
  - PG_USED_FOR_ASSERTS_ONLY (annotation for debug-only variables)
- Called from (representative examples):
  - AtPrepare_PgStat (main prepare phase statistics handler)

## Notes and Other Information
- Only processes top-level transactions (nest_level == 1) during prepare phase
- Creates persistent 2PC records that survive server restarts during prepared transaction state
- Records include both current transaction counters and pre-truncate/drop backup counters
- Uses Assert statements to validate transaction structure assumptions
- Each record is registered with a fixed size of sizeof(TwoPhasePgStatRecord)
- The statistics data will be applied or discarded during the subsequent commit/abort phase
- Essential for maintaining statistics consistency in distributed transaction scenarios
- Handles both regular tables and shared system tables via the shared flag