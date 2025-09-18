# cost_sort

## Location
[src/backend/optimizer/path/costsize.c:2124-2154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L2124-L2154)

## Overview
Determines and returns the cost of sorting a relation, including the cost of reading the input data.

## Definition


## Detailed Description
This is a high-level wrapper function for sorting cost estimation that combines the pure sorting cost (calculated by cost_tuplesort) with the cost of reading input data. It provides the complete cost estimation for a Sort plan node.

The function:
1. Calls cost_tuplesort() to get the base sorting costs
2. Adds the input cost (cost of reading/producing the input data)
3. Applies a penalty (disable_cost) if sorting is disabled via enable_sort parameter
4. Sets the final costs in the provided Path structure

This is the primary interface used throughout the PostgreSQL query planner for estimating sort operation costs.

## Parameters / Member Variables
- : Output parameter - Path object to store the calculated costs and row estimates
- : PlannerInfo containing planner state and statistics (currently unused)
- : List of sort keys (currently unused but reserved for future enhancements)
- : Cost of producing/reading the input data to be sorted
- : Number of tuples in the relation to be sorted
- : Average tuple width in bytes
- : Extra cost per comparison beyond the default
- : Amount of work memory available for sorting (in kilobytes)
- : Bound on output tuples; -1 if no limit

## Dependencies
- Functions called/Symbols referenced:
  - [cost_tuplesort](cost_tuplesort.md)
  - Cost (type)
  - enable_sort (global parameter)
  - disable_cost (global parameter)
- Called from (representative examples):
  - [cost_append](cost_append.md)
  - [create_sort_path](create_sort_path.md)
  - [create_merge_append_path](create_merge_append_path.md)
  - [create_unique_path](create_unique_path.md)
  - [create_gather_merge_path](create_gather_merge_path.md)
  - [create_groupingsets_path](create_groupingsets_path.md)
  - [initial_cost_mergejoin](../i/initial_cost_mergejoin.md)
  - [choose_hashed_setop](choose_hashed_setop.md)

## Notes and Other Information
- Currently ignores pathkeys parameter, but designed to handle it gracefully for future enhancements
- The function notes that callers sometimes pass NIL for pathkeys when sort keys aren't conveniently available
- Applies disable_cost penalty when enable_sort is false, allowing the planner to discourage sorting when configured
- Primary entry point for sort costing throughout the query planner
- Simple wrapper that delegates actual sorting cost calculation to cost_tuplesort