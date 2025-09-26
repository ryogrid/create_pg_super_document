# SubqueryScanPath

## Location
[src/include/nodes/pathnodes.h:1849-1853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L1849-L1853)

## Overview
SubqueryScanPath represents an access path for scanning an unflattened subquery in the FROM clause, enabling the planner to treat subqueries as separate planning domains with their own execution paths.

## Definition

```c
typedef struct SubqueryScanPath
{
	Path		path;
	Path	   *subpath;		/* path representing subquery execution */
} SubqueryScanPath;
```
## Detailed Description
SubqueryScanPath is a specialized scan path node used when PostgreSQL's query planner cannot flatten a subquery into the main query (due to complexity, aggregation, LIMIT clauses, etc.). This path type encapsulates both the outer scan operation and the inner subquery execution plan, allowing the planner to optimize each domain separately.

The subpath comes from a different planning context where RTE (Range Table Entry) indexes and other identifiers have different meanings from those in the outer query. The path.parent->subroot field contains the planning context needed to properly interpret the subpath.

This approach is essential for handling complex subqueries that cannot be merged with the outer query, such as those containing aggregates, window functions, DISTINCT, LIMIT/OFFSET, or set operations.

## Parameters / Member Variables
- : Base Path structure containing common path information including cost estimates, parent relation, pathkeys, and parallel execution properties
- : Path representing the execution plan for the subquery itself, created within the subquery's own planning domain

## Dependencies
- Functions called/Symbols referenced:
  - [Path](../P/Path.md) (base structure)
  - [Path](../P/Path.md) (for subpath)
- Called from (representative examples):
  - [create_subqueryscan_path](../c/create_subqueryscan_path.md) (creates SubqueryScanPath instances)
  - [create_subqueryscan_plan](../c/create_subqueryscan_plan.md) (converts SubqueryScanPath to execution plan)
  - [cost_subqueryscan](../c/cost_subqueryscan.md) (calculates execution costs)
  - [reparameterize_path](../r/reparameterize_path.md) (handles parameter changes)

## Notes and Other Information
- Used only for subqueries that cannot be flattened into the main query
- Supports parallel execution when the subquery path is parallel-safe
- Requires careful handling of parameter passing between outer and inner planning contexts
- The subpath is created using a different PlannerInfo context (subroot)
- Can maintain pathkeys from the subquery when beneficial for upper-level operations
- Handles both lateral references and outer variable substitution through nestloop parameters