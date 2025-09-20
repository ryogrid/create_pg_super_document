# genericcostestimate

## Location
[src/backend/utils/adt/selfuncs.c:6610-6832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6610-L6832)

## Overview
A general-purpose cost estimation function for index access paths that provides basic cost calculations for startup, total cost, selectivity, and correlation that can be used by specific index access methods.

## Definition

```c
void
genericcostestimate(PlannerInfo *root,
					IndexPath *path,
					double loop_count,
					GenericCosts *costs)
```
## Detailed Description
The  function provides a comprehensive cost model for index operations that serves as a foundation for more specialized index access method cost estimators. It calculates various cost components including disk I/O costs, CPU costs, and selectivity estimates.

The function performs several key calculations:
- Estimates the number of index tuples and pages that will be accessed
- Calculates disk I/O costs using the Mackert-Lohman formula to account for cache effects
- Computes CPU costs for evaluating index qualifiers and operators
- Handles ScalarArrayOpExpr operations that result in multiple index scans
- Applies partial index predicates to improve selectivity estimates

The cost model considers nested loop scenarios where the index scan may be repeated multiple times, applying cache-aware algorithms to estimate realistic I/O costs rather than assuming every page access results in a disk read.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and statistics
- : IndexPath structure describing the specific index access path being costed
- : Expected number of times this index scan will be executed (for nested loops)
- : GenericCosts output structure to store the calculated cost estimates

## Dependencies
- Functions called/Symbols referenced:
  - [get_quals_from_indexclauses](get_quals_from_indexclauses.md)
  - [add_predicate_to_index_quals](../a/add_predicate_to_index_quals.md)
  - [estimate_array_length](../e/estimate_array_length.md)
  - [clauselist_selectivity](../c/clauselist_selectivity.md)
  - [get_tablespace_page_costs](get_tablespace_page_costs.md)
  - [index_pages_fetched](../i/index_pages_fetched.md)
  - [index_other_operands_eval_cost](../i/index_other_operands_eval_cost.md)
  - lsecond
- Called from (representative examples):
  - [btcostestimate](../b/btcostestimate.md)
  - [hashcostestimate](../h/hashcostestimate.md)
  - [gistcostestimate](gistcostestimate.md)
  - [spgcostestimate](../s/spgcostestimate.md)

## Notes and Other Information
- Sets index correlation to 0.0 as a generic assumption, though specific index types may override this
- Handles both single scans and multiple scans from ScalarArrayOpExpr operations
- Uses the Mackert-Lohman formula for cache-aware I/O cost estimation when multiple scans are involved
- Estimates are primarily focused on leaf page access costs; upper tree level costs are left to specific index AM implementations
- The function provides a baseline that specific index access methods can build upon or override as needed