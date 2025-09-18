# BuildRelationExtStatistics

## Location
[src/backend/statistics/extended_stats.c:112-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L112-L264)

## Overview
BuildRelationExtStatistics computes and stores extended statistics objects for a relation based on sampled data, handling various types of multi-column statistics including n-distinct, dependencies, MCV lists, and expression statistics.

## Definition


## Detailed Description
This function serves as the main entry point for building extended statistics during the ANALYZE command. It fetches extended statistics definitions from pg_statistic_ext catalog, validates that the required columns have been analyzed, and computes the requested statistics types. For each statistics object, it determines an appropriate statistics target, builds the requested statistics (n-distinct, dependencies, MCV lists, or expression statistics), and stores the results back into the system catalogs. The function also provides progress reporting during extended statistics computation.

The function operates in a temporary memory context to manage memory efficiently during statistics computation, resetting the context after each statistics object is processed.

## Parameters / Member Variables
- : The relation for which to build extended statistics
- : Whether to include inheritance hierarchy statistics  
- : Total number of rows in the relation
- : Number of sampled rows available for computation
- : Array of sampled HeapTuple data
- : Number of attributes being analyzed
- : Array of per-column statistics information

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_statentries_for_relation](../f/fetch_statentries_for_relation.md)
  - [lookup_var_attr_stats](../l/lookup_var_attr_stats.md)
  - [statext_compute_stattarget](../s/statext_compute_stattarget.md)
  - [make_build_data](../m/make_build_data.md)
  - statext_ndistinct_build
  - [statext_dependencies_build](../s/statext_dependencies_build.md)
  - [statext_mcv_build](../s/statext_mcv_build.md)
  - [compute_expr_stats](../c/compute_expr_stats.md)
  - [statext_store](../s/statext_store.md)
  - AllocSetContextCreate
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md)
- Called from:
  - [do_analyze_rel](../d/do_analyze_rel.md) (in src/backend/commands/analyze.c:605)

## Notes and Other Information
- Returns early if no columns are being analyzed (natts == 0)
- Skips statistics objects that cannot be computed due to missing column analysis
- Issues warnings for incomputable statistics objects unless running in autovacuum
- Respects statistics target of 0 by preserving existing statistics values
- Uses a dedicated memory context for efficient memory management
- Provides progress reporting through the statistics analysis progress infrastructure
- Handles four types of extended statistics: NDISTINCT, DEPENDENCIES, MCV, and EXPRESSIONS