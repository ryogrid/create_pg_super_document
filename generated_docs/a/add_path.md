# add_path

## Location
[src/backend/optimizer/util/pathnode.c:420-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L420-L641)

## Overview
Evaluates a potential execution path for a relation and adds it to the relation's pathlist if it offers better cost, ordering, or row count than existing paths with similar parameterization.

## Definition


## Detailed Description
This function implements the core path comparison and selection logic in PostgreSQL's cost-based optimizer. It decides whether to accept a new path by comparing it against existing paths using multiple criteria:

1. **Cost comparison**: Uses fuzzy cost comparison (with STD_FUZZ_FACTOR) to handle floating-point precision issues
2. **Pathkey comparison**: Considers sort order quality, with parameterized paths treated as having no pathkeys
3. **Parameterization**: Compares required outer relations using bms_subset_compare
4. **Row count**: Fewer rows is generally better
5. **Parallel safety**: Parallel-safe paths are preferred

The function maintains the pathlist sorted by total_cost for performance. It removes dominated old paths and may reject the new path if an existing path dominates it. Special handling prevents deletion of IndexPath objects that may be referenced by BitmapHeapPaths.

Key policies:
- Parameterized paths are treated as having NIL pathkeys to reduce kept paths
- Startup cost is only considered interesting based on parent_rel flags
- Memory management includes immediate pfree of discarded paths (except IndexPaths)

## Parameters / Member Variables
- : RelOptInfo structure containing the pathlist to modify
- : Potential new path to evaluate for inclusion

## Dependencies
- Functions called/Symbols referenced:
  - compare_path_costs_fuzzily
  - [compare_pathkeys](../c/compare_pathkeys.md)  
  - [bms_subset_compare](../b/bms_subset_compare.md)
  - PATH_REQ_OUTER
  - foreach_delete_current
  - foreach_current_index
  - [list_insert_nth](../l/list_insert_nth.md)
  - STD_FUZZ_FACTOR (constant)
  - COSTS_EQUAL, COSTS_BETTER1, COSTS_BETTER2, COSTS_DIFFERENT (enum values)
  - PATHKEYS_BETTER1, PATHKEYS_BETTER2, PATHKEYS_DIFFERENT (enum values)
  - BMS_EQUAL, BMS_SUBSET1, BMS_SUBSET2 (enum values)
  - [IndexPath](../I/IndexPath.md) (type)
- Called from (representative examples):
  - [set_plain_rel_pathlist](../s/set_plain_rel_pathlist.md)
  - [create_index_paths](../c/create_index_paths.md)
  - [try_nestloop_path](../t/try_nestloop_path.md)
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [try_hashjoin_path](../t/try_hashjoin_path.md)
  - [generate_gather_paths](../g/generate_gather_paths.md)
  - [create_tidscan_paths](../c/create_tidscan_paths.md)
  - [generate_union_paths](../g/generate_union_paths.md)

## Notes and Other Information
This function is called frequently throughout path generation and serves as a central chokepoint for memory management and plan quality. The fuzzy cost comparison helps avoid platform-specific plan variations due to floating-point roundoff errors. The function includes CHECK_FOR_INTERRUPTS() to allow query cancellation during lengthy planning phases.