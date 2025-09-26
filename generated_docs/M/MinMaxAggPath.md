# MinMaxAggPath

## Location
src/include/nodes/pathnodes.h: 2308 - 2313

## Overview
MinMaxAggPath represents a specialized path for computing MIN/MAX aggregates directly from indexes, providing an optimized execution strategy that avoids scanning the entire table.

## Definition


## Detailed Description
MinMaxAggPath is an optimization path node used when PostgreSQL can compute MIN and MAX aggregate functions directly from index information rather than scanning the entire table. This path type is employed when the query planner determines that the required MIN/MAX values can be obtained by reading the first or last entries from a sorted index, significantly reducing I/O and computation costs. This optimization is particularly effective for queries that only need MIN/MAX aggregates without complex grouping.

## Parameters / Member Variables
- : Base Path structure containing common path information including cost estimates and expected output properties
- : List of MinMaxAggInfo structures, each describing a MIN or MAX aggregate that can be computed from index data
- : List of qualification expressions (HAVING clauses) to be applied after the MIN/MAX computation

## Dependencies
- Functions called/Symbols referenced:
  - MinMaxAggInfo (referenced in mmaggregates list)
- Called from (representative examples):
  - create_minmaxagg_plan
  - create_minmaxagg_path
  - create_plan_recurse

## Notes and Other Information
- This path type enables significant performance improvements for queries with MIN/MAX aggregates by leveraging index ordering
- The optimization is only applicable when indexes exist that can provide the required ordering for MIN/MAX computation
- Multiple MIN/MAX aggregates can be handled in a single MinMaxAggPath through the mmaggregates list
- HAVING quals are still supported and applied after the index-based MIN/MAX computation