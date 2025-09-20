# plan_union_children

## Location
[src/backend/optimizer/prep/prepunion.c:1208-1271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L1208-L1271)

## Overview
Recursively flattens nested UNION operations with identical properties into a single N-way UNION and plans the constituent child queries.

## Definition

```c
static List *
plan_union_children(PlannerInfo *root,
					SetOperationStmt *top_union,
					List *refnames_tlist,
					List **tlist_list,
					List **istrivial_tlist)
```
## Detailed Description
This function implements an optimization for nested UNION operations by flattening identical UNIONs into a single multi-way operation. The process involves:

1. **Tree Traversal**: Uses a pending list to iteratively process UNION nodes, starting with the top-level operation
2. **Identity Check**: For each SetOperationStmt encountered, checks if it has identical properties to the top_union:
   - Same operation type (UNION)  
   - Compatible ALL flags (UNION ALL can be pulled into UNION since distinct output eliminates duplicates anyway)
   - Identical column types and collations
3. **Flattening**: When identical UNIONs are found, their left and right children are added to the pending list instead of planning the UNION itself
4. **Child Planning**: Non-identical operations are planned separately using  with resjunk columns disallowed to ensure consistent output format
5. **Result Assembly**: Returns a list of RelOptInfos for leaf queries and non-identical setops, along with parallel lists of target lists and trivial-tlist flags

This optimization reduces the number of Append/MergeAppend nodes in the final plan and can improve performance.

## Parameters / Member Variables
- : PlannerInfo containing the global planning context and configuration
- : The top-level SetOperationStmt that defines the properties for flattening compatibility
- : List of reference names used for target list construction
- : Output parameter returning list of target lists for each planned child
- : Output parameter returning list of boolean flags indicating trivial target lists

## Dependencies
- Functions called/Symbols referenced:
  - list_make1
  - linitial
  - list_delete_first
  - IsA
  - [equal](../e/equal.md)
  - [lcons](../l/lcons.md)
  - [recurse_set_operations](../r/recurse_set_operations.md)
  - lappend
  - lappend_int
- Called from (representative examples):
  - [generate_union_paths](../g/generate_union_paths.md)

## Notes and Other Information
- The function can pull UNION ALL operations into UNION operations because the distinct output will eliminate duplicates anyway
- Resjunk columns are explicitly disallowed in child results to ensure uniform output format for the Append node
- The flattening optimization only applies to UNIONs with identical column types, collations, and compatible ALL flags
- The algorithm uses a work list approach to handle arbitrary depths of nested identical UNIONs
- This optimization is particularly beneficial for queries with deeply nested UNION operations that would otherwise create multiple levels of Append nodes