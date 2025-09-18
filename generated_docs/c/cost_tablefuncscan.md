# cost_tablefuncscan

## Location
src/backend/optimizer/path/costsize.c: 1592 - 1647

## Overview
Determines and returns the cost of scanning a table function, calculating costs for accessing results from table functions like XMLTABLE, JSON_TABLE, etc.

## Definition


## Detailed Description
The `cost_tablefuncscan` function calculates the cost of scanning a table function (such as XMLTABLE, JSON_TABLE, or other table-generating functions) by evaluating the cost of executing the table function expression and adding the overhead of tuple processing and qualification checking. Similar to regular function scans, the table function evaluation cost is treated as startup cost, reflecting that the function is typically executed to completion before returning results. The function accounts for the cost of evaluating table function expressions, applying any restriction clauses, and processing the target list. Like `cost_functionscan`, it does not currently account for tuplestore spill costs, acknowledging that row count estimates for table functions are often imprecise.

## Parameters / Member Variables
- `path`: Output parameter where the calculated costs will be stored
- `root`: PlannerInfo structure containing global planner state  
- `baserel`: RelOptInfo for the table function relation
- `param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized paths

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - cost_qual_eval_node
  - get_restriction_qual_cost
  - RTE_TABLEFUNC (constant)
- Called from (representative examples):
  - create_tablefuncscan_path

## Notes and Other Information
- Only applies to base relations that are table functions (RTE_TABLEFUNC)
- Table function evaluation cost is treated as startup cost due to typical execution patterns
- Does not currently account for tuplestore spill costs, similar to regular function scans
- Row count estimates for table functions are often imprecise, affecting cost accuracy
- Target list evaluation costs are applied per output row, not per scanned tuple
- Handles both parameterized and non-parameterized table function scans
- Used for table functions like XMLTABLE, JSON_TABLE, and other SQL standard table functions