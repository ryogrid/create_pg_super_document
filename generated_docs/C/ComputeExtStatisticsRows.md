# ComputeExtStatisticsRows

## Location
[src/backend/statistics/extended_stats.c:265-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L265-L346)

## Overview
ComputeExtStatisticsRows calculates the number of sample rows needed for computing extended statistics on a relation, considering only statistics objects that can actually be built with the available column analysis.

## Definition

```c
int
ComputeExtStatisticsRows(Relation onerel,
						 int natts, VacAttrStats **vacattrstats)
```
## Detailed Description
This function determines the sample size requirements for extended statistics computation during ANALYZE. It examines all extended statistics objects defined for the relation, checks whether each can be computed with the currently analyzed columns, calculates the statistics target for each valid object, and returns a sample size based on the highest target found. The function uses a simple formula of 300 rows per statistics target unit to determine the required sample size. This preprocessing step allows ANALYZE to collect sufficient samples before attempting to build the actual extended statistics.

## Parameters / Member Variables
- `onerel`: The relation being analyzed
- `natts`: Number of attributes being analyzed
- `**vacattrstats`: Array of per-column statistics information for analyzed attributes
## Dependencies
- Functions called/Symbols referenced:
  - [fetch_statentries_for_relation](../f/fetch_statentries_for_relation.md)
  - [lookup_var_attr_stats](../l/lookup_var_attr_stats.md)
  - [statext_compute_stattarget](../s/statext_compute_stattarget.md)
  - [bms_num_members](../b/bms_num_members.md)
  - AllocSetContextCreate
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from:
  - [do_analyze_rel](../d/do_analyze_rel.md) (in src/backend/commands/analyze.c:510)

## Notes and Other Information
- Returns 0 if no columns are being analyzed
- Skips statistics objects that cannot be computed with available columns
- Uses the maximum statistics target among all valid statistics objects
- Applies a fixed multiplier of 300 to convert statistics target to sample row count
- Uses a temporary memory context for safe memory management
- Does not report warnings for incomputable statistics (deferred to BuildRelationExtStatistics)
- The 300x multiplier ensures sufficient sample size for accurate extended statistics computation

## Simplified Source

```c
int ComputeExtStatisticsRows(Relation onerel, int natts, VacAttrStats **vacattrstats) {
    Relation pg_stext;
    List *lstats;
    MemoryContext cxt, oldcxt;
    int result = 0;

    // Return 0 if no columns to analyze
    if (!natts)
        return 0;

    // Create memory context for computation
    cxt = AllocSetContextCreate(CurrentMemoryContext, "ComputeExtStatisticsRows", ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(cxt);

    // Get list of extended statistics objects for this relation
    pg_stext = table_open(StatisticExtRelationId, RowExclusiveLock);
    lstats = fetch_statentries_for_relation(pg_stext, RelationGetRelid(onerel));

    // Process each statistics object to find maximum target
    foreach(lc, lstats) {
        StatExtEntry *stat = (StatExtEntry *) lfirst(lc);
        VacAttrStats **stats;
        int stattarget;
        int nattrs = bms_num_members(stat->columns);

        // Check if we can build this statistics object with available columns
        stats = lookup_var_attr_stats(onerel, stat->columns, stat->exprs, natts, vacattrstats);
        if (!stats)
            continue;  // Skip if required columns aren't analyzed

        // Compute statistics target for this object
        stattarget = statext_compute_stattarget(stat->stattarget, nattrs, stats);

        // Keep track of the largest target
        if (stattarget > result)
            result = stattarget;
    }

    // Clean up
    table_close(pg_stext, RowExclusiveLock);
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(cxt);

    // Return sample size: 300 rows per statistics target unit
    return (300 * result);
}
```