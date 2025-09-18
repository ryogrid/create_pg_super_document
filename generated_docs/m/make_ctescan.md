# make_ctescan

## Location
src/backend/optimizer/plan/createplan.c: 5763 - 5783

## Overview
Creates and initializes a CteScan plan node, which represents a scan operation on a Common Table Expression (CTE) that has been materialized and stored for reuse within a query.

## Definition
```c
static CteScan *
make_ctescan(List *qptlist,
             List *qpqual,
             Index scanrelid,
             int ctePlanId,
             int cteParam)
```

## Detailed Description
The `make_ctescan` function is a factory function that constructs a CteScan plan node. This node type is used when the query planner needs to scan the results of a Common Table Expression (CTE) defined with a WITH clause. CTEs can be referenced multiple times within a query, and when PostgreSQL determines it's beneficial to materialize the CTE results, subsequent references use CteScan nodes to read from the materialized data. The function allocates memory for a new CteScan node, initializes its base Plan structure, and sets up the CTE-specific identifiers needed to locate and access the materialized CTE data.

## Parameters / Member Variables
- `qptlist`: The target list (projection list) specifying which columns/expressions to return from the CTE scan
- `qpqual`: The qualification conditions (WHERE clause predicates) to be applied during the scan
- `scanrelid`: The relation ID assigned to this scan operation for identification purposes
- `ctePlanId`: Identifier for the specific CTE plan that produced the materialized data to be scanned
- `cteParam`: Parameter ID used for accessing the materialized CTE data during execution

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate CteScan node)
  - CteScan (node type)
- Called from (representative examples):
  - [create_ctescan_plan](../c/create_ctescan_plan.md)

## Notes and Other Information
- This is a static function within createplan.c, indicating it's an internal helper for plan creation
- The function follows PostgreSQL's pattern of setting lefttree and righttree to NULL for leaf scan nodes
- CTEs can be either materialized (stored temporarily) or inlined depending on the planner's cost-based decisions
- The ctePlanId and cteParam work together to uniquely identify and access the correct materialized CTE data
- CteScan nodes are essential for implementing recursive CTEs and for optimizing queries with multiple CTE references
- Part of PostgreSQL's query planner infrastructure that handles WITH clause functionality and CTE materialization