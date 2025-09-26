# WorkTableScan

## Location
[src/include/nodes/plannodes.h:661-665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L661-L665)

## Overview
WorkTableScan is a plan node type that scans the working table of a recursive Common Table Expression (CTE) during recursive query execution.

## Definition

```c
typedef struct WorkTableScan
{
	Scan		scan;
	int			wtParam;		/* ID of Param representing work table */
} WorkTableScan;
```
## Detailed Description
WorkTableScan is a specialized scan node used in PostgreSQL's recursive CTE implementation. It inherits from the base Scan structure and adds a single parameter to identify the working table parameter. During recursive CTE execution, PostgreSQL uses a working table (tuplestore) to store intermediate results from each iteration of the recursive process. The WorkTableScan node provides access to this working table for the recursive arm of the CTE. The node operates by locating the appropriate RecursiveUnion ancestor through the parameter mechanism and then scanning the tuples stored in the working table.

## Parameters / Member Variables
- : Base Scan structure containing common scan fields (Plan plan, Index scanrelid)
- : Integer ID of the execution parameter that represents the work table, used to locate the RecursiveUnion state containing the working table tuplestore

## Dependencies
- Functions called/Symbols referenced:
  - Scan (base structure)
- Called from (representative examples):
  - ExecInitNode (plan node initialization dispatcher)
  - ExecWorkTableScan (main execution function)
  - ExecInitWorkTableScan (initialization function)
  - create_worktablescan_plan (plan creation)
  - make_worktablescan (plan node construction)
  - set_plan_refs (plan reference setting)
  - ExecReScanWorkTableScan (rescan functionality)

## Notes and Other Information
- WorkTableScan nodes are created specifically for recursive CTE processing
- The working table is implemented as a tuplestore managed by the parent RecursiveUnion node
- Initialization is deferred until execution time due to timing dependencies with ancestor nodes
- The scan produces unordered results as work tables contain computed intermediate data
- Used in conjunction with RecursiveUnion nodes to implement SQL recursive queries
- No parallel execution support as recursive CTEs require sequential processing
- The wtParam field is crucial for locating the correct working table when multiple recursive CTEs are nested