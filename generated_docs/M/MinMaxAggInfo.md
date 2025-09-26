# MinMaxAggInfo

## Location
[src/include/nodes/pathnodes.h:3107-3136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L3107-L3136)

## Overview
MinMaxAggInfo describes a potentially index-optimizable MIN/MAX aggregate function, storing the metadata needed to implement efficient index-based lookups instead of full table scans for MIN/MAX operations.

## Definition
```c
typedef struct MinMaxAggInfo
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag     type;

    /* pg_proc Oid of the aggregate */
    Oid         aggfnoid;

    /* Oid of its sort operator */
    Oid         aggsortop;

    /* expression we are aggregating on */
    Expr       *target;

    /*
     * modified "root" for planning the subquery; not printed, too large, not
     * interesting enough
     */
    PlannerInfo *subroot pg_node_attr(read_write_ignore);

    /* access path for subquery */
    Path       *path;

    /* estimated cost to fetch first row */
    Cost        pathcost;

    /* param for subplan's output */
    Param      *param;
} MinMaxAggInfo;
```

## Detailed Description
MinMaxAggInfo is a key data structure in PostgreSQL's aggregate optimization system, specifically designed to enable index-based MIN/MAX aggregate computation. Instead of scanning entire tables to find minimum or maximum values, PostgreSQL can use indexes to efficiently locate the first or last values in sorted order.

This structure is created during the aggregate preprocessing phase when the planner identifies MIN/MAX aggregates that can potentially be optimized using existing indexes. The optimization works by converting the aggregate into a subquery that uses index scans to directly fetch the minimum or maximum value.

When a MinMaxAggPath containing these structures is accepted during planning, the list is stored in root->minmax_aggs for later use during the setrefs.c phase, where references are resolved and the final plan structure is built.

The optimization is particularly effective for queries like "SELECT MIN(column) FROM table" where an appropriate index exists on the column, allowing PostgreSQL to avoid scanning the entire table.

## Parameters / Member Variables
- : Standard NodeTag for node type identification
- : Object ID (OID) of the aggregate function from pg_proc catalog (e.g., MIN or MAX function)
- : Object ID of the sort operator used by this aggregate to determine ordering semantics
- : The expression being aggregated (the argument to MIN/MAX function)
- : Modified PlannerInfo structure for planning the subquery that will replace the aggregate (marked with read_write_ignore attribute)
- : The access path (typically an index scan) that will be used in the subquery to efficiently find the MIN/MAX value
- : Estimated cost to fetch the first row using the chosen access path
- : Parameter node representing the subplan's output in the larger query plan

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node type system)
  - [Expr](../E/Expr.md) (expression node)  
  - [PlannerInfo](../P/PlannerInfo.md) (planner state)
  - [Path](../P/Path.md) (access path)
  - Cost (cost estimation type)
  - [Param](../P/Param.md) (parameter node)

- Called from (representative examples):
  - [preprocess_minmax_aggregates](../p/preprocess_minmax_aggregates.md) (in planagg.c:154, 195)
  - [build_minmax_path](../b/build_minmax_path.md) (in planagg.c:316)
  - [create_minmaxagg_path](../c/create_minmaxagg_path.md) (in pathnode.c:3427)
  - [create_minmaxagg_plan](../c/create_minmaxagg_plan.md) (in createplan.c:2560)
  - [can_minmax_aggs](../c/can_minmax_aggs.md) (in planagg.c:250, 293)
  - [find_minmax_agg_replacement_param](../f/find_minmax_agg_replacement_param.md) (in setrefs.c:3449)

## Notes and Other Information
- Uses pg_node_attr with no_copy_equal, no_read, no_query_jumble attributes to control node processing behavior
- The subroot field is marked with read_write_ignore to exclude it from certain node operations due to its size and complexity
- Critical for achieving O(log n) performance for MIN/MAX queries instead of O(n) table scans when appropriate indexes are available
- Part of PostgreSQL's sophisticated aggregate optimization framework that can dramatically improve performance for common analytical queries
- The optimization is only applicable when suitable indexes exist and the aggregate can be safely converted to an index-based lookup