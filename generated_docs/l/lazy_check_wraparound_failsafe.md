# lazy_check_wraparound_failsafe

## Location
[src/backend/access/heap/vacuumlazy.c:2300-2352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2300-L2352)

## Overview
Triggers a failsafe mechanism to prevent transaction ID wraparound failure by checking if the table's relfrozenxid or relminmxid is dangerously far in the past and bypassing non-essential vacuum operations.

## Definition

```c
static bool
lazy_check_wraparound_failsafe(LVRelState *vacrel)
```
## Detailed Description
This function implements a critical safety mechanism in PostgreSQL's vacuum system to prevent transaction ID wraparound disasters. It checks whether the table's frozen transaction ID (relfrozenxid) or minimum multixact ID (relminmxid) has fallen dangerously behind, indicating that the table is at risk of transaction ID wraparound.

When the failsafe triggers, it immediately disables several non-essential but time-consuming vacuum operations: index vacuuming, index cleanup, and heap relation truncation. This allows the vacuum to focus only on the most critical work - advancing the table's relfrozenxid and relminmxid values to prevent wraparound.

The function also removes cost-based delays and buffer access strategy limitations to speed up the remaining critical work. It provides detailed warning messages to help administrators understand the situation and take corrective action.

## Parameters / Member Variables
- `*vacrel`: LVRelState structure containing vacuum operation state, relation information, and transaction ID cutoff values used for wraparound detection
## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_xid_failsafe_check](../v/vacuum_xid_failsafe_check.md)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md)
  - ereport (for WARNING messages)
- Called from:
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [lazy_scan_heap](lazy_scan_heap.md)  
  - [lazy_vacuum_all_indexes](lazy_vacuum_all_indexes.md) (multiple locations)

## Notes and Other Information
- Returns true when failsafe has been triggered, false otherwise
- Uses VacuumFailsafeActive global flag to prevent multiple warnings per VACUUM
- Sets VacuumFailsafeActive to true when triggered to affect the entire vacuum operation
- Disables buffer access strategy by setting bstrategy to NULL 
- Turns off cost-based vacuum delays by setting VacuumCostActive to false
- Resets progress counters for index processing since those operations are bypassed
- Issues WARNING with detailed explanation including database, schema, and table names
- Provides hints about increasing maintenance_work_mem or autovacuum_work_mem
- Critical for preventing database shutdown due to transaction ID wraparound
- Should trigger only in emergency situations when normal vacuum maintenance has fallen behind

## Simplified Source

```c
static bool
lazy_check_wraparound_failsafe(LVRelState *vacrel)
{
    // Don't warn more than once per VACUUM
    if (VacuumFailsafeActive)
        return true;

    // Check if we're in danger of transaction ID wraparound
    if (unlikely(vacuum_xid_failsafe_check(&vacrel->cutoffs)))
    {
        VacuumFailsafeActive = true;

        // Abandon buffer access strategy to use all shared buffers
        vacrel->bstrategy = NULL;

        // Disable non-essential operations
        vacrel->do_index_vacuuming = false;
        vacrel->do_index_cleanup = false;
        vacrel->do_rel_truncate = false;

        // Reset progress counters
        pgstat_progress_update_multi_param(2,
            (int[]){PROGRESS_VACUUM_INDEXES_TOTAL, PROGRESS_VACUUM_INDEXES_PROCESSED},
            (int64[]){0, 0});

        // Warn administrator about the failsafe activation
        ereport(WARNING,
                (errmsg("bypassing nonessential maintenance of table \"%s.%s.%s\" "
                        "as a failsafe after %d index scans",
                        vacrel->dbname, vacrel->relnamespace, vacrel->relname,
                        vacrel->num_index_scans),
                 errdetail("The table's relfrozenxid or relminmxid is too far in the past."),
                 errhint("Consider increasing configuration parameter \"maintenance_work_mem\" or \"autovacuum_work_mem\".\n"
                        "You might also need to consider other ways for VACUUM to keep up with the allocation of transaction IDs.")));

        // Disable cost-based delays to speed up remaining work
        VacuumCostActive = false;
        VacuumCostBalance = 0;

        return true;
    }

    return false;
}
```