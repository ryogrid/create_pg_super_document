# make_subqueryscan

## Location
[src/backend/optimizer/plan/createplan.c:5684-5703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5684-L5703)

## Overview
Creates and initializes a SubqueryScan plan node, which represents a scan operation on the result of a subquery in PostgreSQL's query execution plan.

## Definition

```c
static SubqueryScan *
make_subqueryscan(List *qptlist,
				  List *qpqual,
				  Index scanrelid,
				  Plan *subplan)
```
## Detailed Description
The `make_subqueryscan` function is a factory function that constructs a SubqueryScan plan node. This node type is used when the query planner needs to scan the results of a subquery. The function allocates memory for a new SubqueryScan node, initializes its base Plan structure with the provided target list and qualification conditions, and sets up the subquery-specific fields including the scan relation ID and the subplan that will produce the data to be scanned.

The function sets the scan status to `SUBQUERY_SCAN_UNKNOWN` initially, indicating that the execution engine hasn't yet determined the optimal scanning strategy for this subquery.

## Parameters / Member Variables
- `qptlist`: The target list (projection list) specifying which columns/expressions to return from the subquery scan
- `qpqual`: The qualification conditions (WHERE clause predicates) to be applied during the scan
- `scanrelid`: The relation ID assigned to this scan operation for identification purposes
- `subplan`: The child plan node that represents the subquery to be executed and scanned

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate SubqueryScan node)
  - [SubqueryScan](../S/SubqueryScan.md) (node type)
  - SUBQUERY_SCAN_UNKNOWN (initial scan status constant)
- Called from (representative examples):
  - [create_subqueryscan_plan](../c/create_subqueryscan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's an internal helper for plan creation
- The function follows PostgreSQL's pattern of setting lefttree and righttree to NULL for leaf scan nodes
- The scanstatus field allows the execution engine to optimize subquery scanning based on runtime characteristics
- Part of PostgreSQL's query planner infrastructure that transforms logical query plans into executable physical plans