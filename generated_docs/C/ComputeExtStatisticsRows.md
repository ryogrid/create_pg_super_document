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
- : The relation being analyzed
- : Number of attributes being analyzed 
- : Array of per-column statistics information for analyzed attributes

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