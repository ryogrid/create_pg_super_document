# inline_cte

## Location
[src/backend/optimizer/plan/subselect.c:1138-1150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L1138-L1150)

## Overview
Converts all RTE_CTE (Range Table Entry for Common Table Expression) references to a specific CTE into RTE_SUBQUERY entries by performing an in-place substitution throughout the query tree.

## Definition

```c
struct inline_cte_walker_context context;
```
## Detailed Description
This function serves as the entry point for CTE inlining optimization in PostgreSQL's query planner. It prepares a walker context structure and initiates a tree traversal to replace all references to a specific CTE with inline subqueries. CTE inlining is a crucial optimization technique that can eliminate the materialization overhead of CTEs when they are referenced only once or when inlining would be beneficial for performance.

The function sets up an  structure containing the CTE's name, the current nesting level (starting at -1 since it will be immediately incremented), and the CTE's query tree that will be substituted. It then delegates the actual traversal and replacement work to , which recursively processes the entire query tree starting from the root parse tree.

The levelsup counter is initialized to -1 because the walker function will increment it before processing, ensuring proper level tracking for nested queries and subquery references.

## Parameters / Member Variables
- : PlannerInfo structure containing the query tree and planner state information
- : CommonTableExpr structure representing the CTE to be inlined, containing its name and query definition

## Dependencies
- Functions called/Symbols referenced:
  - [inline_cte_walker](inline_cte_walker.md)
  - castNode (macro for safe type casting)
  - [inline_cte_walker_context](inline_cte_walker_context.md) (context structure)
- Called from (representative examples):
  - [SS_process_ctes](../S/SS_process_ctes.md)

## Notes and Other Information
- This function is part of the CTE optimization process in PostgreSQL's query planner
- Inlining CTEs can improve performance by eliminating materialization costs and enabling further optimizations
- The levelsup tracking is essential for correctly handling nested queries and ensuring CTE references are found at the right scope level
- Static function scope restricts access to the subselect.c compilation unit
- The function modifies the query tree in-place, performing destructive updates to convert CTE references to subqueries
- CTE inlining is typically applied when the CTE is referenced only once or when cost analysis suggests inlining would be beneficial