# preprocess_minmax_aggregates

## Location
[src/backend/optimizer/plan/planagg.c:72-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planagg.c#L72-L235)

## Overview
Preprocesses MIN/MAX aggregate functions to determine if they can be optimized via index scans, creating a MinMaxAggPath when optimization is possible.

## Definition


## Detailed Description
This function analyzes queries containing MIN/MAX aggregate functions to determine if they can be optimized using index scans instead of full table scans. It performs several validation checks to ensure the query structure is compatible with the optimization:

1. **Query Structure Validation**: Rejects queries with GROUP BY clauses, multiple grouping sets, window functions, CTEs, or complex joins
2. **Table Restrictions**: Only handles queries referencing exactly one table (including inheritance hierarchies and flattened UNION ALL subqueries)
3. **Aggregate Analysis**: Verifies all aggregates are MIN/MAX functions via 
4. **Index Path Building**: Attempts to build index paths for each aggregate using 
5. **Path Creation**: Creates a  node with estimated costs and adds it to the  upperrel for cost comparison

The optimization works by using index scans to directly fetch the minimum or maximum values without scanning all table rows, providing significant performance improvements for eligible queries.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and state information

## Dependencies
- Functions called/Symbols referenced:
  -  - Validates that all aggregates are MIN/MAX functions
  -  - Attempts to build index scan paths for each aggregate
  -  - Gets equality operator for aggregate's ordering operator
  -  - Creates output parameters for aggregates
  -  - Creates the MinMaxAggPath node
  -  - Retrieves the GROUP_AGG upperrel
  -  - Adds the path to the relation for cost comparison
- Called from (representative examples):
  -  (src/backend/optimizer/plan/planner.c:1517)

## Notes and Other Information
- Must be called after  since it relies on 
- Called just before  since it clones planner state for path generation  
- Creates PARAM_EXEC slots for each aggregate even if the optimization isn't ultimately used
- MinMaxAggPath nodes are currently never parallel-safe
- The optimization is most effective for queries like  where  has suitable indexes