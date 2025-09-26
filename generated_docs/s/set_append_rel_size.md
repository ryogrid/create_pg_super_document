# set_append_rel_size

## Location
[src/backend/optimizer/path/allpaths.c:944-1231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L944-L1231)

## Overview
Sets size estimates for an append relation by computing aggregate statistics across all child relations, handling inheritance trees and partitioned tables.

## Definition

```c
structures as well.  This is needed either if the parent
		 * participates in some eclass joins (because we will want to consider
		 * inner-indexscan joins on the individual children) or if the parent
		 * has useful pathkeys (because we should try to build MergeAppend
		 * paths that produce those sort orderings).
		 */
		if (rel->has_eclass_joins || has_useful_pathkeys(root, rel))
			add_child_rel_equivalences(root, appinfo, rel, childrel);
```
## Detailed Description
This function computes size estimates for append relations, which represent tables accessed through inheritance hierarchies or partitioned tables. It processes each child relation in the append relation list, applies constraint exclusion to eliminate children that cannot produce rows, and aggregates size statistics from all remaining live children.

The function handles several key responsibilities: it sets up partitionwise join consideration for partitioned base relations, copies the parent's targetlist and join quals to each child with appropriate variable substitutions, manages equivalence class relationships for join optimization, determines parallel safety across all children, and computes weighted size estimates based on each child's contribution.

Width estimates are computed by weighting child relation widths proportionally to their row counts, which provides accurate footprint estimates for operations like sorting or hashing. The function also handles constraint exclusion at the child level and can mark children as dummy if they're proven to produce no rows.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner information and append relation list
- : RelOptInfo structure for the append relation that will be updated with aggregated size estimates
- : Range table index of the parent relation in the append hierarchy
- : RangeTblEntry for the parent relation containing relation metadata

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (prevents stack overflow in deep inheritance trees)
  - IS_SIMPLE_REL (checks if relation is a simple base relation)
  - bms_is_empty (checks if bitmap set is empty)
  - [find_base_rel](../f/find_base_rel.md) (locates child relation's RelOptInfo)
  - IS_DUMMY_REL (checks if relation produces no rows)
  - [relation_excluded_by_constraints](../r/relation_excluded_by_constraints.md) (applies constraint exclusion)
  - [set_dummy_rel_pathlist](set_dummy_rel_pathlist.md) (marks relation as producing no rows)
  - [bms_overlap](../b/bms_overlap.md) (checks bitmap set overlap for nulling relations)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md) (translates expressions between parent and child)
  - [has_useful_pathkeys](../h/has_useful_pathkeys.md) (checks if relation has useful sort orders)
  - [add_child_rel_equivalences](../a/add_child_rel_equivalences.md) (creates equivalence class entries)
  - [set_rel_consider_parallel](set_rel_consider_parallel.md) (determines parallel safety)
  - [set_rel_size](set_rel_size.md) (recursively computes child relation sizes)
  - forboth (macro for parallel iteration over two lists)
  - [get_typavgwidth](../g/get_typavgwidth.md) (gets average width for a data type)
  - [exprType](../e/exprType.md)/exprTypmod (extracts type information from expressions)

- Called from (representative examples):
  - [set_rel_size](set_rel_size.md) (main relation sizing dispatcher)

## Notes and Other Information
- This function is static and only used within allpaths.c
- Guards against stack overflow in deeply nested inheritance hierarchies
- Supports both traditional inheritance and partitioned table hierarchies
- Implements constraint exclusion to eliminate impossible child relations
- Handles partitionwise join setup for partitioned base relations
- Manages parallel safety propagation - if any child is not parallel-safe, the whole append relation is marked unsafe
- Width estimation uses row-weighted averaging across all live children
- Leaves rel->pages as zero to avoid double-counting in total_table_pages
- Can result in a dummy append relation if all children are excluded by constraints
- The function assumes child RelOptInfo structures have already been created during add_other_rels_to_query
- [Variable](../V/Variable.md) substitution between parent and child uses the AppendRelInfo translation mappings