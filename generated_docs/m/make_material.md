# make_material

## Location
[src/backend/optimizer/plan/createplan.c:6506-6527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6506-L6527)

## Overview
Creates a Material plan node that materializes the output of its child plan, storing the results for potential multiple accesses.

## Definition


## Detailed Description
This function constructs a Material plan node, which is used to materialize (store in memory or on disk) the output of its child plan. Materialization is useful when the same data needs to be accessed multiple times, such as in certain join operations or when a subplan needs to be rescanned. The function creates a new Material node using makeNode(), copies the target list from the child plan, sets the qualification to NIL (no additional filtering), and establishes the parent-child relationship.

The Material node acts as a buffer between its child and parent plans, allowing the child's output to be stored and retrieved multiple times without re-executing the child plan.

## Parameters / Member Variables
- : The input Plan node whose output will be materialized

## Dependencies
- Functions called/Symbols referenced:
  - Material (struct type, created with makeNode())
- Called from (representative examples):
  - [create_material_plan](../c/create_material_plan.md)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md)
  - [materialize_finished_plan](materialize_finished_plan.md)

## Notes and Other Information
- This is a static function, accessible only within the same source file
- The Material node has no right child (righttree is set to NULL)
- No qualification conditions are applied (qual is set to NIL)
- The target list is directly copied from the child plan
- Material nodes are often used in scenarios where rescanning is required
- Located in src/backend/optimizer/plan/createplan.c at lines 6506-6527