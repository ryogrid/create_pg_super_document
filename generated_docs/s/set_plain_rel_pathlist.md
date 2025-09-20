# set_plain_rel_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:764-793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L764-L793)

## Overview
Generates all possible access paths for a plain base relation, including sequential scans, parallel scans, index scans, and TID scans.

## Definition

```c
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```
## Detailed Description
This function creates the complete set of access paths for plain base relations in PostgreSQL's query optimizer. It systematically generates different types of scan paths that can be used to access the relation's data. The function handles the core access methods available for regular tables: sequential scanning, parallel sequential scanning (when appropriate), index-based scanning, and tuple-ID (TID) scanning.

The function first determines any required parameterization due to LATERAL references in the target list, then creates a sequential scan path as the baseline access method. If parallel processing is enabled for the relation and no outer parameters are required, it generates partial paths for parallel execution. Finally, it considers index-based access paths and TID scan paths.

The generated paths are added to the relation's pathlist, where the query optimizer will later evaluate their costs and choose the most efficient access strategy during plan generation.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and planning context
- : RelOptInfo structure representing the relation for which to build access paths
- : RangeTblEntry containing parse tree information about the relation (currently unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [add_path](../a/add_path.md) (adds sequential scan path to relation's pathlist)
  - [create_seqscan_path](../c/create_seqscan_path.md) (creates sequential scan access path)
  - [create_plain_partial_paths](../c/create_plain_partial_paths.md) (generates parallel scan paths)
  - [create_index_paths](../c/create_index_paths.md) (generates index-based access paths)
  - [create_tidscan_paths](../c/create_tidscan_paths.md) (creates TID scan access paths)
- Called from:
  - [set_rel_pathlist](set_rel_pathlist.md) (main pathlist generation dispatcher)

## Notes and Other Information
- This is a static function within allpaths.c that handles the simplest case of base relation access
- The function always creates a sequential scan path as a fallback option
- Parallel paths are only generated when the relation is marked as consider_parallel and has no required outer parameters
- LATERAL references can create parameterization requirements that affect path generation
- Join clauses cannot be pushed down into sequential scan quals, but parameterization due to LATERAL refs is supported
- The function represents the core access path generation for regular tables, excluding specialized relation types
- Generated paths will be costed and compared during the path selection phase
- TID scans are considered for relations that might be accessed via tuple ID equality conditions