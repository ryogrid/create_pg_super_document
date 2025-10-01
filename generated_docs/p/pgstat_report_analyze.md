# pgstat_report_analyze

## Location
[src/backend/utils/activity/pgstat_relation.c:277-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L277-L359)

## Overview
Reports that a table has been analyzed and updates the relation's statistics with adjusted live/dead tuple counts, handling transaction-level modifications to avoid double-counting.

## Definition
void pgstat_report_analyze(Relation rel, PgStat_Counter livetuples, PgStat_Counter deadtuples, bool resetcounter)

## Detailed Description
This function updates statistics after an ANALYZE operation completes on a table. Unlike VACUUM, ANALYZE may run within a transaction that has already modified the target table, so this function performs sophisticated accounting to avoid double-counting tuples. It walks through all transaction levels to subtract out modifications that will be reported separately at transaction commit.

The function adjusts the provided live and dead tuple estimates by subtracting transaction-level changes (inserts, updates, deletes) and aborted subtransaction effects. It then updates the shared statistics with the corrected counts, optionally resets the modification counter, and records timing information for scheduling future ANALYZE operations.

## Parameters / Member Variables
- : The Relation structure for the table that was analyzed
- : Estimated count of live tuples after analysis
- : Estimated count of dead tuples after analysis  
- : Boolean flag indicating whether to reset the mod_since_analyze counter

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_should_count_relation
  - [pgstat_get_entry_ref_locked](pgstat_get_entry_ref_locked.md)
  - AmAutoVacuumWorkerProcess
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [pgstat_unlock_entry](pgstat_unlock_entry.md)
  - [pgstat_flush_io](pgstat_flush_io.md)
  - PGSTAT_KIND_RELATION
  - PgStat_Counter
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md)
  - [PgStatShared_Relation](../P/PgStatShared_Relation.md)
  - [PgStat_StatTabEntry](../P/PgStat_StatTabEntry.md)
  - [PgStat_TableXactStatus](../P/PgStat_TableXactStatus.md)
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md)
  - pgstat_count_conn_txn_idle_time

## Notes and Other Information
- Only operates when pgstat_track_counts is enabled
- Performs complex transaction-aware tuple count adjustments to prevent double-counting of modifications made during the ANALYZE operation
- Skips transaction adjustments for partitioned tables since they don't store data directly
- Uses Max() function to ensure counts don't underflow due to estimation inaccuracies
- Updates different timestamp and counter fields based on whether the process is autovacuum or manual analyze
- Optionally resets mod_since_analyze counter, which affects scheduling of future analyze operations
- Immediately flushes IO statistics similar to vacuum operations
- Handles both shared and non-shared relations appropriately

## Simplified Source

```c
void pgstat_report_analyze(Relation rel, PgStat_Counter livetuples,
                          PgStat_Counter deadtuples, bool resetcounter) {
    PgStat_EntryRef *entry_ref;
    PgStatShared_Relation *shtabentry;
    PgStat_StatTabEntry *tabentry;
    Oid dboid = (rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId);

    // Early return if stats tracking is disabled
    if (!pgstat_track_counts)
        return;

    // Adjust counts for modifications made during ANALYZE to avoid double-counting
    if (pgstat_should_count_relation(rel) &&
        rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE) {

        // Walk through all transaction levels to subtract out modifications
        for (PgStat_TableXactStatus *trans = rel->pgstat_info->trans;
             trans; trans = trans->upper) {
            livetuples -= trans->tuples_inserted - trans->tuples_deleted;
            deadtuples -= trans->tuples_updated + trans->tuples_deleted;
        }

        // Subtract dead tuples from aborted subtransactions
        deadtuples -= rel->pgstat_info->counts.delta_dead_tuples;

        // Ensure counts don't underflow due to estimation errors
        livetuples = Max(livetuples, 0);
        deadtuples = Max(deadtuples, 0);
    }

    // Get locked reference to statistics entry
    entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION, dboid,
                                           RelationGetRelid(rel), false);
    Assert(entry_ref != NULL && entry_ref->shared_stats != NULL);

    shtabentry = (PgStatShared_Relation *) entry_ref->shared_stats;
    tabentry = &shtabentry->stats;

    // Update live and dead tuple counts
    tabentry->live_tuples = livetuples;
    tabentry->dead_tuples = deadtuples;

    // Reset modification counter if requested
    if (resetcounter)
        tabentry->mod_since_analyze = 0;

    // Update timing and count based on whether this is autovacuum or manual analyze
    if (AmAutoVacuumWorkerProcess()) {
        tabentry->last_autoanalyze_time = GetCurrentTimestamp();
        tabentry->autoanalyze_count++;
    } else {
        tabentry->last_analyze_time = GetCurrentTimestamp();
        tabentry->analyze_count++;
    }

    pgstat_unlock_entry(entry_ref);

    // Flush IO statistics similar to vacuum
    pgstat_flush_io(false);
}
```