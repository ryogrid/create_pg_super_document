# indexed_tlist

## Location
[src/backend/optimizer/plan/setrefs.c:55-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L55-L61)

## Overview
A structure that provides indexed access to target list entries, optimizing lookups of variables and expressions by caching commonly accessed information about target list elements.

## Definition

```c
typedef struct
{
	PlannerInfo *root;
	int			rtoffset;
	double		num_exec;
} fix_scan_expr_context;
```
## Detailed Description
The  structure is a performance optimization used in PostgreSQL's query planner to provide efficient access to target list entries during expression fixing phases. It acts as an indexed view over a target list, pre-computing and caching information about variable references to avoid repeated linear searches through the target list.

This structure is particularly important during the set_plan_refs phase of query planning, where the planner needs to replace variable references with appropriate target list references. By maintaining an indexed structure, the planner can quickly locate target list entries that match specific variable criteria.

The structure includes flags to indicate the presence of different types of expressions (PlaceHolderVars, non-variable expressions) and maintains an array of  structures that provide quick access to plain Var entries.

## Parameters / Member Variables
- `tlist`: The underlying target list that this structure indexes
- `num_vars`: Count of plain Var entries in the target list, determines size of vars array
- `has_ph_vars`: Boolean flag indicating whether the target list contains PlaceHolderVar entries
- `has_non_vars`: Boolean flag indicating whether the target list contains non-variable expressions
- `vars`: Flexible array member containing tlist_vinfo structures for quick Var lookups

## Dependencies
- Functions called/Symbols referenced:
  - tlist_vinfo (companion structure)
  - [List](../L/List.md) (PostgreSQL list type)
- Called from (representative examples):
  - [build_tlist_index](../b/build_tlist_index.md)
  - [fix_scan_expr_context](../f/fix_scan_expr_context.md)
  - [fix_join_expr_context](../f/fix_join_expr_context.md)
  - [fix_upper_expr_context](../f/fix_upper_expr_context.md)
  - [search_indexed_tlist_for_var](../s/search_indexed_tlist_for_var.md)
  - [search_indexed_tlist_for_phv](../s/search_indexed_tlist_for_phv.md)

## Notes and Other Information
- This is a performance optimization structure used internally by the query planner
- The flexible array member  contains exactly  entries
- Used extensively during expression reference fixing in query plan generation
- Helps avoid O(n) linear searches through target lists by providing O(1) indexed access
- The structure is typically built once and then used for multiple lookups during plan reference resolution