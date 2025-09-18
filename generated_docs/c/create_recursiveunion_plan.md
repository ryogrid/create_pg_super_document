# create_recursiveunion_plan

## Location
[src/backend/optimizer/plan/createplan.c:2756-2791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2756-L2791)

## Overview
Creates a RecursiveUnion plan node for recursive Common Table Expression (CTE) queries, building plans for both the non-recursive and recursive parts of the query.

## Definition


## Detailed Description
This function constructs a RecursiveUnion plan node that implements recursive CTEs in PostgreSQL. Recursive CTEs consist of two parts: a non-recursive term (left plan) and a recursive term (right plan). The function creates plans for both subpaths and ensures they produce compatible target lists using the CP_EXACT_TLIST flag. It builds the target list for the union operation and configures the RecursiveUnion node with parameters needed for execution, including the working table parameter and distinctness requirements.

The RecursiveUnion node is essential for implementing SQL's WITH RECURSIVE functionality, which allows queries to reference themselves and process hierarchical or graph-like data structures.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and information needed for plan generation
- : RecursiveUnionPath representing the chosen execution strategy for the recursive union, containing left and right subpaths, working table parameters, and cardinality estimates

## Dependencies
- Functions called/Symbols referenced:
  - [create_plan_recurse](create_plan_recurse.md)
  - [build_path_tlist](../b/build_path_tlist.md)
  - [clamp_cardinality_to_long](clamp_cardinality_to_long.md)
  - [make_recursive_union](../m/make_recursive_union.md)
  - [copy_generic_path_info](copy_generic_path_info.md)
  - CP_EXACT_TLIST (flag constant)
- Called from (representative examples):
  - [create_plan_recurse](create_plan_recurse.md)

## Notes and Other Information
- The function is static, indicating it's only used within the createplan.c module
- Both child plans must produce identical target lists, enforced by the CP_EXACT_TLIST flag
- Uses clamp_cardinality_to_long to safely convert cardinality estimates and prevent overflow
- The wtParam (working table parameter) identifies the recursive reference in the recursive term
- Essential component of PostgreSQL's implementation of SQL standard recursive CTEs
- Handles distinctness requirements through the distinctList parameter to eliminate duplicates when needed