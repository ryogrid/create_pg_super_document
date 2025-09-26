# pgstat_report_vacuum

## Location
[src/backend/utils/activity/pgstat_relation.c:211-276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L211-L276)

## Overview
Reports that a table has been vacuumed and updates the relation's statistics with live/dead tuple counts and vacuum timing information.

## Definition
void pgstat_report_vacuum(Oid tableoid, bool shared, PgStat_Counter livetuples, PgStat_Counter deadtuples)

## Detailed Description
This function updates statistics after a VACUUM operation completes on a table. It records the current counts of live and dead tuples, resets the insert counter, and updates vacuum timing and count statistics. The function differentiates between manual VACUUM operations and autovacuum operations, updating the appropriate counters and timestamps for each type.

The function also flushes IO statistics immediately after updating the relation statistics, ensuring that IO metrics from the vacuum operation are reported promptly rather than waiting for the next statistics reporting cycle.

## Parameters / Member Variables
- : Object identifier of the table that was vacuumed
- : Boolean indicating whether this is a shared relation (catalog table)
- : Count of live tuples after the vacuum operation
- : Count of dead tuples remaining after the vacuum operation

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimestamp
  - pgstat_get_entry_ref_locked
  - AmAutoVacuumWorkerProcess
  - pgstat_unlock_entry
  - pgstat_flush_io
  - PGSTAT_KIND_RELATION
  - PgStat_Counter
  - PgStat_EntryRef
  - PgStatShared_Relation
  - PgStat_StatTabEntry
- Called from (representative examples):
  - heap_vacuum_rel
  - pgstat_count_conn_txn_idle_time

## Notes and Other Information
- Only operates when pgstat_track_counts is enabled
- Resets ins_since_vacuum counter to zero regardless of vacuum aggressiveness, which affects insert-triggered autovacuum scheduling
- Updates different timestamp and counter fields depending on whether the process is an autovacuum worker or manual vacuum
- Uses timestamping to track when the last vacuum occurred for scheduling future maintenance
- Immediately flushes IO statistics to ensure timely reporting of vacuum-related IO metrics
- Handles both shared and non-shared relations by determining the appropriate database OID