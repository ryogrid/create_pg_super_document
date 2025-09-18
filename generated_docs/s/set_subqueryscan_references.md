# set_subqueryscan_references

## Location
[src/backend/optimizer/plan/setrefs.c:1395-1463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1395-L1463)

## Overview
Specialized function that processes SubqueryScan plan nodes, with optimization logic to potentially eliminate trivial SubqueryScans by pulling up their subplans directly.

## Definition


## Detailed Description
 handles the unique processing requirements for SubqueryScan nodes, which represent subqueries in the FROM clause. The function implements an important optimization: it attempts to eliminate unnecessary SubqueryScan wrapper nodes when the subquery is trivial and can be safely pulled up into the parent query level.

The function follows this process:
1. Looks up the subquery's RelOptInfo to access its subroot (the subquery's own PlannerInfo)
2. Recursively processes the subplan using the subquery's own planner context via 
3. Checks if the SubqueryScan is trivial using 
4. If trivial: eliminates the SubqueryScan node entirely and pulls up the subplan using 
5. If not trivial: keeps the SubqueryScan and performs standard reference adjustment without using  (since SubqueryScan nodes are created with correct initial references)

This optimization is significant because it can eliminate entire levels of plan nodes, reducing execution overhead and simplifying the plan tree when subqueries don't add meaningful structure.

## Parameters / Member Variables
- : PlannerInfo structure containing the parent query's planner state and context
- : The SubqueryScan node to process and potentially eliminate
- : Integer offset to add to rangetable indices for proper variable resolution

## Dependencies
- Functions called/Symbols referenced:
  - [find_base_rel](../f/find_base_rel.md): Locates the RelOptInfo for the subquery relation
  - [set_plan_references](set_plan_references.md): Recursively processes the subplan with its own planner context
  - [trivial_subqueryscan](../t/trivial_subqueryscan.md): Determines if the SubqueryScan can be safely eliminated
  - [clean_up_removed_plan_level](../c/clean_up_removed_plan_level.md): Handles the mechanics of pulling up a subplan
  - fix_scan_list: Standard variable reference adjustment for scan expressions
  - NUM_EXEC_TLIST/NUM_EXEC_QUAL: Macros for determining execution context
- Called from (representative examples):
  - [set_plan_refs](set_plan_refs.md): When processing SubqueryScan nodes in the main plan tree traversal
  - fix_scan_list: During recursive plan reference adjustment

## Notes and Other Information
The decision not to use  for non-trivial SubqueryScans is important - SubqueryScan nodes are unique in that they're created with correct references to their subplan outputs from the start, unlike other upper nodes that need reference transformation. The function represents a key optimization in PostgreSQL's query planning, as eliminating unnecessary SubqueryScan nodes can significantly improve query performance by reducing the number of tuple passing operations between plan levels. The recursive call to  using the subquery's own subroot ensures that variable references within the subplan are resolved correctly within the subquery's scope.