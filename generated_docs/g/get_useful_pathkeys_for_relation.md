# get_useful_pathkeys_for_relation

## Location
src/backend/optimizer/path/allpaths.c: 3122 - 3189

## Overview
Determines which orderings of a relation might be useful for the query, considering both final output ordering and efficient merge joins, with support for parallel-safe requirements.

## Definition
```c
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel,
                                 bool require_parallel_safe)
```

## Detailed Description
This function analyzes the query's pathkeys (ordering requirements) to determine which orderings would be beneficial for a given relation. It evaluates whether getting data in sorted order would be useful either for matching the final output ordering or enabling efficient merge joins.

The function examines the query_pathkeys list and validates each pathkey to ensure it contains equivalence class members that are safe to compute early and computable from the current relation's reltarget. It supports incremental sort optimization by returning prefixes of the pathkeys list that meet the requirements, even if the full list doesn't qualify.

When parallel execution is involved, the function can enforce parallel-safe requirements on the sort expressions, allowing sorts to be pushed below Gather Merge nodes for parallelized execution.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning information including query_pathkeys
- `rel`: RelOptInfo structure representing the relation being analyzed for useful orderings
- `require_parallel_safe`: Boolean flag requiring sort expressions to be parallel-safe for execution below Gather Merge

## Dependencies
- Functions called/Symbols referenced:
  - relation_can_be_sorted_early
  - list_copy_head
  - PathKey (type)
  - EquivalenceClass (type)
- Called from (representative examples):
  - generate_useful_gather_paths

## Notes and Other Information
- Currently returns at most a single-element list based on query_pathkeys, but designed for future extension
- Supports incremental sort by returning valid prefixes of pathkeys when full list doesn't qualify
- Validates that pathkey equivalence classes are computable from the relation's reltarget
- Optimizes list handling by returning the original query_pathkeys pointer when possible
- Future enhancements may consider pathkeys useful for merge joins beyond query ordering
- Critical for enabling parallelized incremental sorts under Gather Merge nodes