# get_parameterized_joinrel_size

## Location
[src/backend/optimizer/path/costsize.c:5353-5393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L5353-L5393)

## Overview
Estimates the size (number of rows) for a parameterized scan of a join relation, accounting for parameter-dependent selectivity.

## Definition
```c
double get_parameterized_joinrel_size(PlannerInfo *root, RelOptInfo *rel,
                                      Path *outer_path, Path *inner_path,
                                      SpecialJoinInfo *sjinfo, List *restrict_clauses)
```

## Detailed Description
This function provides size estimation specifically for parameterized joins, where the join's selectivity may depend on parameter values from outer query levels. It calculates the estimated number of rows by considering the sizes of the input paths and applying the selectivity of join clauses that will be evaluated at this join node.

The function uses `calc_joinrel_size_estimate` to perform the core calculation, passing the actual row counts from the parameterized input paths rather than the base relation estimates. This allows for more accurate estimation when dealing with parameterized scans that may have significantly different cardinalities than their base relations.

As a safety measure, the result is clamped to not exceed the base relation's row estimate (`rel->rows`), which was previously set by `set_joinrel_size_estimates`. This prevents unrealistic estimates that could occur due to estimation errors in parameterized scenarios.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the query planning process
- `rel`: The join RelOptInfo whose parameterized size is being estimated
- `outer_path`: Path representing the outer relation input to the join (may be parameterized)
- `inner_path`: Path representing the inner relation input to the join (may be parameterized)
- `sjinfo`: SpecialJoinInfo containing information about special join types
- `restrict_clauses`: List of restriction clauses to be applied at this join node

## Dependencies
- Functions called/Symbols referenced:
  - [calc_joinrel_size_estimate](../c/calc_joinrel_size_estimate.md)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md) (struct type)
- Called from (representative examples):
  - get_joinrel_parampathinfo (src/backend/optimizer/util/relnode.c:1834)

## Notes and Other Information
- Must be called after `set_joinrel_size_estimates` has been applied to establish the base estimate
- [Result](../R/Result.md) is clamped to not exceed the base relation's row estimate for safety
- Handles the complexity of parameterized joins where cardinality depends on outer parameter values
- Like other join size estimation functions, results may vary slightly depending on the input path pair provided
- Critical for accurate costing of nested loop joins with parameterized inner paths