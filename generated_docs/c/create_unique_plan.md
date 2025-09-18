# create_unique_plan

## Location
src/backend/optimizer/plan/createplan.c: 1721 - 1919

## Overview
Creates a Unique plan node for eliminating duplicate rows from a subplan, either using sorting or hashing methods.

## Definition


## Detailed Description
The  function generates a plan node that eliminates duplicate rows from its subplan based on the unique expressions specified in the . The function supports two uniquification methods:

1. **Hash-based uniquification (UNIQUE_PATH_HASH)**: Creates an Agg node with AGG_HASHED strategy that groups by the unique expressions, effectively eliminating duplicates through hash-based grouping.

2. **Sort-based uniquification (UNIQUE_PATH_SORT)**: Creates a Sort node followed by a Unique node that eliminates consecutive duplicate rows after sorting.

The function handles target list management carefully, ensuring that any expressions needed for uniquification are added to the subplan's target list. It builds control structures (groupColIdx, groupCollations, groupOperators) that specify which columns to examine and what operators to use for the uniquification process.

A special case is handled when the unique method is UNIQUE_PATH_NOOP, where no actual uniquification is needed and the subplan is returned as-is.

## Parameters / Member Variables
- : PlannerInfo containing planner state and context information
- : UniquePath specifying the uniquification strategy, expressions, and operators to use
- : Control flags passed through to recursive plan creation (e.g., CP_IGNORE_TLIST)

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [tlist_member](../t/tlist_member.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [change_plan_targetlist](change_plan_targetlist.md)
  - [exprCollation](../e/exprCollation.md)
  - [get_compatible_hash_operators](../g/get_compatible_hash_operators.md)
  - [make_agg](../m/make_agg.md)
  - [get_ordering_op_for_equality_op](../g/get_ordering_op_for_equality_op.md)
  - [get_equality_op_for_ordering_op](../g/get_equality_op_for_ordering_op.md)
  - [get_tle_by_resno](../g/get_tle_by_resno.md)
  - [assignSortGroupRef](../a/assignSortGroupRef.md)
  - [make_sort_from_sortclauses](../m/make_sort_from_sortclauses.md)
  - [label_sort_with_costsize](../l/label_sort_with_costsize.md)
  - [make_unique_from_sortclauses](../m/make_unique_from_sortclauses.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function optimizes target list handling by only modifying the subplan's target list when necessary (when new expressions need to be added or when sorting is required)
- For hash-based uniquification, it handles cross-type operators by finding compatible hash operators for the equality comparisons
- For sort-based uniquification, it constructs appropriate SortGroupClause structures with proper ordering and equality operators
- The function preserves parallel safety information when modifying target lists
- Cost information is copied from the UniquePath to the resulting Plan node