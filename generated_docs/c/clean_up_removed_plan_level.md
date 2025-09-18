# clean_up_removed_plan_level

## Location
[src/backend/optimizer/plan/setrefs.c:1534-1577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1534-L1577)

## Overview
Utility function that performs necessary cleanup operations when eliminating a parent plan node and promoting its child plan node to take its place in the plan tree.

## Definition


## Detailed Description
 handles the essential housekeeping tasks required when plan optimization eliminates intermediate plan nodes like SubqueryScan, Append, or MergeAppend nodes. When these wrapper nodes are determined to be unnecessary, the child plan is promoted to replace the parent, but certain attributes must be transferred to maintain correct execution semantics and query output formatting.

The function performs two critical operations:

**InitPlan Management**: Any initialization plans (subqueries that must execute before the main plan) attached to the parent node are transferred to the child. This includes:
- Computing and adding initplan costs to the child's cost estimates
- Checking for parallel-unsafe initplans and updating the child's parallel safety status
- Concatenating initplan lists to preserve execution order (parent's initplans execute first)

**Column Labeling Transfer**: The parent's targetlist column labeling information (column names, junk status, etc.) is applied to the child's targetlist via . This is crucial for proper client-side column identification, especially when the eliminated node was at the topmost plan level.

This cleanup ensures that plan tree optimizations don't lose critical execution metadata or alter the visible query results.

## Parameters / Member Variables
- : The plan node being eliminated from the tree
- : The child plan node that will replace the parent in the tree

## Dependencies
- Functions called/Symbols referenced:
  - [SS_compute_initplan_cost](../S/SS_compute_initplan_cost.md): Calculates cost and safety implications of initplans
  - [list_concat](../l/list_concat.md): Combines parent and child initplan lists
  - [apply_tlist_labeling](../a/apply_tlist_labeling.md): Transfers column labeling from parent to child targetlist
  - Cost: Type for representing query execution costs
- Called from (representative examples):
  - [set_subqueryscan_references](../s/set_subqueryscan_references.md): When eliminating trivial SubqueryScan nodes
  - [set_append_references](../s/set_append_references.md): When simplifying Append nodes with single children
  - [set_mergeappend_references](../s/set_mergeappend_references.md): When simplifying MergeAppend nodes with single children

## Notes and Other Information
The function is essential for maintaining query execution correctness during plan optimization. The initplan transfer logic is particularly important because initplans represent subqueries that must execute exactly once before the main query, and losing them would cause incorrect results. The cost adjustment ensures that query planning decisions remain accurate after node elimination. The column labeling transfer is critical for client applications that depend on proper column metadata - without this step, eliminated top-level nodes would cause columns to appear with internal names rather than the expected user-visible names. The ordering preservation in initplan concatenation is conservative but safe, ensuring that any subtle dependencies between initialization subqueries are maintained.