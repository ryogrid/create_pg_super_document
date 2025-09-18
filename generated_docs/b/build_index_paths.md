# build_index_paths

## Location
src/backend/optimizer/path/indxpath.c: 804 - 1085

## Overview
 constructs zero or more IndexPaths (and partial IndexPaths) for a given index and set of index clauses, supporting both forward and backward scans when beneficial.

## Definition


## Detailed Description
This comprehensive function builds IndexPaths through a systematic 5-step process:

1. **Clause Combination**: Combines per-column IndexClause lists into an overall ordered list (by index key column), handling ScalarArrayOpExpr clauses based on index AM support and caller preferences.

2. **Pathkey Analysis**: Computes pathkeys describing the index's ordering and determines how many are useful for the current query, considering both natural index ordering and distance ordering operators.

3. **Index-Only Scan Check**: Determines if an index-only scan is possible by checking if all required columns are available in the index.

4. **Forward Scan Generation**: Creates IndexPaths for forward scans when there are relevant restriction clauses, useful pathkeys, useful predicates, or index-only scan possibilities. Also considers parallel index scans when appropriate.

5. **Backward Scan Generation**: For ordered indexes, generates backward scan IndexPaths when the reverse ordering would be useful for the query.

The function handles different scan types (ST_INDEXSCAN, ST_BITMAPSCAN, ST_ANYSCAN) and ensures compatibility with the index's access method capabilities.

## Parameters / Member Variables
- : PlannerInfo containing planner state and configuration
- : RelOptInfo representing the heap relation being scanned  
- : IndexOptInfo describing the index for path generation
- : IndexClauseSet containing indexable clauses organized by column
- : Whether the index has a useful predicate for this query context
- : ScanTypeControl indicating desired scan types (plain, bitmap, or both)
- : Optional flag to skip ScalarArrayOpExpr clauses unsupported by index AM

## Dependencies
- Functions called/Symbols referenced:
  - [create_index_path](../c/create_index_path.md)
  - [build_index_pathkeys](build_index_pathkeys.md)
  - [check_index_only](../c/check_index_only.md)
  - [match_pathkeys_to_index](../m/match_pathkeys_to_index.md)
  - [get_loop_count](../g/get_loop_count.md)
  - [has_useful_pathkeys](../h/has_useful_pathkeys.md)
  - [truncate_useless_pathkeys](../t/truncate_useless_pathkeys.md)
  - [add_partial_path](../a/add_partial_path.md)
- Called from (representative examples):
  - [get_index_paths](../g/get_index_paths.md)
  - [build_paths_for_OR](build_paths_for_OR.md)

## Notes and Other Information
- Returns paths to caller rather than immediately submitting them via add_path()
- Handles both regular and parallel index scans when conditions are met
- Supports incremental sort by matching prefixes of query pathkeys to index ordering
- Enforces amoptionalkey restrictions for indexes that require at least one matching clause
- The function can return an empty list if no viable paths can be constructed
- Parallel index scans are not supported for bitmap scans