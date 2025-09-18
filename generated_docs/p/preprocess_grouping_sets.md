# preprocess_grouping_sets

## Location
[src/backend/optimizer/plan/planner.c:2077-2257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L2077-L2257)

## Overview
Performs preprocessing for GROUPING SETS clauses by expanding grouping sets, organizing them into rollup structures, and preparing annotations for cost estimation.

## Definition
```c
static grouping_sets_data *preprocess_grouping_sets(PlannerInfo *root)
```

## Detailed Description
This function is responsible for the complex preprocessing of GROUPING SETS clauses in PostgreSQL. It handles the transformation from the raw parse tree representation into organized structures suitable for execution planning.

Key operations include:
1. **Expansion**: Uses expand_grouping_sets to expand complex grouping set specifications
2. **Classification**: Separates columns into hashable/unhashable and sortable/unsortable categories
3. **Validation**: Ensures that unsortable grouping sets are still hashable (required constraint)
4. **Organization**: Groups related grouping sets into rollups for efficient execution
5. **Reordering**: Orders grouping sets optimally, considering ORDER BY clauses when possible
6. **Mapping**: Creates index mappings from sort group references to column positions

The function creates a grouping_sets_data structure containing all the information needed by later planning phases, including rollup data for sortable sets and separate handling for unsortable (hash-only) sets.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query planning context

## Dependencies
- Functions called/Symbols referenced:
  - [expand_grouping_sets](../e/expand_grouping_sets.md), extract_rollup_sets, reorder_grouping_sets
  - [preprocess_groupclause](preprocess_groupclause.md), remap_to_groupclause_idx
  - [bms_add_member](../b/bms_add_member.md), bms_overlap_list, bms_is_empty
  - makeNode (GroupingSetData, RollupData)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md)

## Notes and Other Information
- Located in src/backend/optimizer/plan/planner.c:2077-2257
- This is a static function that serves as a key component of GROUPING SETS planning
- The function enforces the constraint that unsortable sets must be hashable, throwing an error if violated
- When only one aggregation pass is needed, the function tries to match the ORDER BY clause for efficiency
- Creates separate handling paths for sortable sets (organized into rollups) and unsortable sets (hash-only)
- The tleref_to_colnum_map workspace array is used for remapping sort group references to column indices
- Sets processed_groupClause to the original groupClause when grouping sets are present (no optimization currently performed)