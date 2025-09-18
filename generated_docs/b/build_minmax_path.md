# build_minmax_path

## Location
src/backend/optimizer/plan/planagg.c: 316 - 477

## Overview
Attempts to build an optimized index scan path for a single MIN/MAX aggregate by creating a specialized subquery with LIMIT 1.

## Definition


## Detailed Description
This function constructs an optimized execution path for MIN/MAX aggregates by creating a modified subquery that can leverage index scans. The process involves:

1. **Subquery Construction**: Clones the current planner state and increments the query level to create an isolated subquery environment
2. **Query Transformation**: Converts the aggregate into a simple SELECT with:
   - Single target list entry for the aggregate column
   - IS NOT NULL condition on the target column
   - ORDER BY clause using the provided sort operator and null handling
   - LIMIT 1 to fetch only the minimum/maximum value
3. **Path Planning**: Invokes  with  to generate optimal paths
4. **Path Selection**: Chooses the cheapest fractional path for the required sort order
5. **Cost Calculation**: Computes the cost to retrieve just the first row from the sorted path

The function effectively transforms  into , allowing the optimizer to use index scans instead of full table scans.

## Parameters / Member Variables
- : PlannerInfo structure containing the current query planning context
- : MinMaxAggInfo structure that will be populated with the generated path information
- : OID of the equality operator corresponding to the sort operator
- : OID of the sort operator for ordering (ASC for MIN, DESC for MAX)
- : Boolean indicating whether NULL values should be sorted first

## Dependencies
- Functions called/Symbols referenced:
  -  - Main query planning function to generate paths
  -  - Callback function to customize query planning behavior
  -  - Selects optimal path for required ordering
  -  - Adjusts path to return correct target list
  -  - Handles parameter references in subquery
  -  - Adjusts costs for initialization plans
  -  - Adjusts variable reference levels for subquery
  -  - Creates sort group reference for ORDER BY clause
- Called from (representative examples):
  -  (src/backend/optimizer/plan/planagg.c:175, 177)

## Notes and Other Information
- Returns true if a suitable index path is found, false otherwise
- The function tries both NULLS FIRST and NULLS LAST orderings to find the best available index
- Generated subquery becomes an initplan since it has no level-1 variables after transformation
- Path costs are calculated to match  methodology
- Assumes the target expression was already validated as non-mutable and non-rowtype
- The IS NOT NULL condition is only added if not already present in the WHERE clause