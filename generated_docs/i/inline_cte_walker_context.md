# inline_cte_walker_context

## Location
[src/backend/optimizer/plan/subselect.c:60-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L60-L65)

## Overview
A context structure used during Common Table Expression (CTE) inlining to track the target CTE information and provide the replacement query during tree traversal.

## Definition

```c
typedef struct inline_cte_walker_context
{
	const char *ctename;		/* name and relative level of target CTE */
	int			levelsup;
	Query	   *ctequery;		/* query to substitute */
} inline_cte_walker_context;
```
## Detailed Description
The  structure serves as a parameter context for the  function during the process of inlining Common Table Expressions (CTEs). This structure is used in PostgreSQL's query optimizer when converting RTE_CTE (Range Table Entry for CTE) references into RTE_SUBQUERY references, effectively replacing CTE references with their actual query definitions. The context maintains the identity of the CTE being inlined, tracks the current nesting level during traversal, and provides the replacement query. This transformation is part of PostgreSQL's CTE optimization strategy, where non-recursive CTEs can be inlined to enable better optimization opportunities.

## Parameters / Member Variables
- : String containing the name of the target CTE that should be inlined during the tree walk
- : Integer tracking the current nesting level during query tree traversal, used to match CTE references at the correct scope level and adjust variable references appropriately
- : Pointer to the Query structure that represents the CTE definition and will be used as a replacement for matching CTE references

## Dependencies
- Functions called/Symbols referenced:
  - [Query](../Q/Query.md) (structure)
- Called from (representative examples):
  - [inline_cte](inline_cte.md) (src/backend/optimizer/plan/subselect.c:1140)
  - [inline_cte_walker](inline_cte_walker.md) (src/backend/optimizer/plan/subselect.c:1151)

## Notes and Other Information
- This context structure is part of PostgreSQL's CTE optimization infrastructure, specifically for inlining non-recursive CTEs
- The  field is crucial for correctly matching CTE references at the appropriate scope level in nested queries
- Used during query tree traversal to systematically replace RTE_CTE entries with RTE_SUBQUERY entries
- The inlining process includes proper adjustment of variable reference levels when the CTE is inlined at different nesting depths
- Part of the query rewriting phase that can enable further optimizations by converting CTEs into subqueries
- The context ensures that only the target CTE (matching both name and level) is inlined, preserving other CTE references
- Supports the optimization strategy where simple CTEs are inlined to allow for better join planning and other optimizations