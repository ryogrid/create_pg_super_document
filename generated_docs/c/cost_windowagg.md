# cost_windowagg

## Location
[src/backend/optimizer/path/costsize.c:3068-3162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L3068-L3162)

## Overview
Determines and returns the cost of performing a WindowAgg plan node, including the cost of its input data which is assumed already properly sorted.

## Definition

```c
void
cost_windowagg(Path *path, PlannerInfo *root,
			   List *windowFuncs, WindowClause *winclause,
			   Cost input_startup_cost, Cost input_total_cost,
			   double input_tuples)
```
## Detailed Description
This function calculates the execution cost for a WindowAgg operation in PostgreSQL's query planner. It estimates the cost by considering several factors:

1. **Window function execution costs**: Each window function in the list is evaluated for its execution cost plus input expression evaluation cost per tuple. The cost estimation assumes window functions will evaluate their inputs once per cycle, which may underestimate actual costs when functions fetch multiple rows.

2. **Grouping comparison costs**: Adds cpu_operator_cost for each partitioning and ordering column per tuple to account for grouping comparisons.

3. **General overhead**: Includes cpu_tuple_cost per tuple for general processing overhead.

4. **Startup cost adjustment**: Calculates how many tuples need to be read from the subnode to produce the first output tuple, proportionally distributing the run cost over startup tuples when more than one tuple is needed.

The function does not account for spooling costs when data overflows work_mem, which is noted as a future enhancement.

## Parameters / Member Variables
- : The Path structure to be updated with calculated costs and row estimates
- : PlannerInfo structure containing planner context and statistics
- : List of WindowFunc nodes representing the window functions to be executed
- : WindowClause specifying partitioning and ordering requirements
- : Startup cost of the input data source
- : Total cost of the input data source
- : Estimated number of input tuples

## Dependencies
- Functions called/Symbols referenced:
  - [add_function_cost](../a/add_function_cost.md)
  - [cost_qual_eval_node](cost_qual_eval_node.md)
  - [get_windowclause_startup_tuples](../g/get_windowclause_startup_tuples.md)
  - WindowClause
  - WindowFunc
  - QualCost
  - Cost
- Called from (representative examples):
  - [create_windowagg_path](create_windowagg_path.md)

## Notes and Other Information
- Input data is assumed to be already properly sorted according to the window clause requirements
- The cost estimation for window functions may be conservative as it assumes single evaluation per cycle
- Future enhancement needed: accounting for disk spooling costs when work_mem is exceeded
- The startup cost calculation considers the window clause requirements to determine how many tuples must be processed before the first output tuple can be produced