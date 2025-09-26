# make_worktablescan

## Location
[src/backend/optimizer/plan/createplan.c:5804-5822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5804-L5822)

## Overview
Creates and initializes a WorkTableScan plan node for scanning working tables used in recursive Common Table Expressions (CTEs) in PostgreSQL.

## Definition
```c
static WorkTableScan *
make_worktablescan(List *qptlist,
                   List *qpqual,
                   Index scanrelid,
                   int wtParam)
```

## Detailed Description
This function constructs a WorkTableScan plan node, which is specifically designed to scan working tables during the execution of recursive CTEs. Working tables are temporary storage areas used to hold intermediate results during recursive query processing. The wtParam parameter identifies which working table parameter to use from the execution context, allowing multiple recursive operations to coexist.

## Parameters / Member Variables
- `qptlist`: Target list specifying which columns/expressions to return from the working table scan
- `qpqual`: List of qualification conditions (WHERE clause predicates) to apply during scanning
- `scanrelid`: Index identifying the relation being scanned in the query's range table
- `wtParam`: Parameter ID identifying the specific working table to scan in the execution context

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate WorkTableScan node)
  - [WorkTableScan](../W/WorkTableScan.md) (struct type)
- Called from (representative examples):
  - [create_worktablescan_plan](../c/create_worktablescan_plan.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the createplan.c file
- Working tables are essential for implementing recursive CTEs efficiently by providing a mechanism to store and iterate over intermediate results
- The wtParam allows the executor to distinguish between different working tables when multiple recursive operations are nested or running concurrently
- Like other scan nodes, this is a leaf node in the plan tree with no child nodes