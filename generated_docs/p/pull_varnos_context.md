# pull_varnos_context

## Location
[src/backend/optimizer/util/var.c:37-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L37-L42)

## Overview
A context structure used by the pull_varnos functionality to collect relation IDs (varnos) from Var nodes in an expression tree during query planning optimization.

## Definition

```c
typedef struct
{
	Bitmapset  *varattnos;
	Index		varno;
} pull_varattnos_context;
```
## Detailed Description
The pull_varnos_context structure serves as a walker context for the pull_varnos_walker function, which traverses expression trees to identify all relation IDs referenced by Var nodes. This is a critical part of PostgreSQL's query optimization process, where the planner needs to understand which relations are being accessed by a particular expression or subquery. The context accumulates relation IDs while maintaining awareness of the current subquery level being processed.

## Parameters / Member Variables
- : A Bitmapset containing the collected relation IDs found during the tree walk
- : Pointer to the PlannerInfo structure containing planner state and context information
- : Integer tracking the current subquery nesting level being processed (0 for current level)

## Dependencies
- Functions called/Symbols referenced:
  - Relids (typedef)
  - [PlannerInfo](../P/PlannerInfo.md) (struct)
  - [Bitmapset](../B/Bitmapset.md) (typedef)
- Called from (representative examples):
  - [pull_varnos](pull_varnos.md)
  - [pull_varnos_of_level](pull_varnos_of_level.md)
  - [pull_varnos_walker](pull_varnos_walker.md)
  - flatten_join_alias_vars_context

## Notes and Other Information
This context structure is specifically designed for tree walking operations where the goal is to collect relation identifiers. The sublevels_up field is crucial for handling correlated subqueries correctly, ensuring that variables are only collected from the appropriate query level. The structure works in conjunction with PostgreSQL's expression tree walker framework to provide a systematic way to extract relation dependencies from complex query expressions.