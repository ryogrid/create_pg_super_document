# set_append_rel_size

## Location
src/backend/optimizer/path/allpaths.c: 944 - 1231

## Overview
Sets size estimates for an append relation by computing aggregate statistics across all child relations, handling inheritance trees and partitioned tables.

## Definition


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
  - check_stack_depth (prevents stack overflow in deep inheritance trees)
  - IS_SIMPLE_REL (checks if relation is a simple base relation)
  - bms_is_empty (checks if bitmap set is empty)
  - find_base_rel (locates child relation's RelOptInfo)
  - IS_DUMMY_REL (checks if relation produces no rows)
  - relation_excluded_by_constraints (applies constraint exclusion)
  - set_dummy_rel_pathlist (marks relation as producing no rows)
  - bms_overlap (checks bitmap set overlap for nulling relations)
  - adjust_appendrel_attrs (translates expressions between parent and child)
  - has_useful_pathkeys (checks if relation has useful sort orders)
  - add_child_rel_equivalences (creates equivalence class entries)
  - set_rel_consider_parallel (determines parallel safety)
  - set_rel_size (recursively computes child relation sizes)
  - forboth (macro for parallel iteration over two lists)
  - get_typavgwidth (gets average width for a data type)
  - exprType/exprTypmod (extracts type information from expressions)

- Called from (representative examples):
  - set_rel_size (main relation sizing dispatcher)

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
- Variable substitution between parent and child uses the AppendRelInfo translation mappings