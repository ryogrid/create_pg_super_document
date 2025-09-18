# cost_tidrangescan

## Location
[src/backend/optimizer/path/costsize.c:1357-1450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1357-L1450)

## Overview
Determines and sets the costs of scanning a relation using a range of TIDs, providing cost estimation for TID range scan operations that can access contiguous blocks of tuples.

## Definition


## Detailed Description
The `cost_tidrangescan` function calculates the cost of performing a TID range scan, which is an access method that can scan a contiguous range of tuple identifiers rather than individual TIDs. This is more efficient than individual TID lookups when scanning multiple consecutive tuples. The costing model accounts for the fact that the first page requires a random seek, but subsequent pages in the range can be accessed sequentially. The function uses selectivity estimation to determine how many pages and tuples will be accessed, then applies both random and sequential page costs appropriately. The design intentionally makes TID range scans cost slightly more than equivalent sequential scans to prefer sequential scans when they offer benefits like scan synchronization and parallelization.

## Parameters / Member Variables
- `path`: Output parameter where the calculated costs will be stored
- `root`: PlannerInfo structure containing global planner state
- `baserel`: RelOptInfo for the relation being scanned
- `tidrangequals`: List of TID range qualification clauses
- `param_info`: ParamPathInfo for parameterized paths, or NULL for non-parameterized paths

## Dependencies
- Functions called/Symbols referenced:
  - [clauselist_selectivity](clauselist_selectivity.md)
  - [cost_qual_eval](cost_qual_eval.md)
  - [get_tablespace_page_costs](../g/get_tablespace_page_costs.md)
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md)
  - JOIN_INNER (constant)
- Called from (representative examples):
  - [create_tidrangescan_path](create_tidrangescan_path.md)

## Notes and Other Information
- Only applies to base relations, not joins or subqueries
- Uses selectivity estimation to determine the expected page and tuple counts
- First page incurs random I/O cost, subsequent pages use sequential I/O cost
- Intentionally costs slightly higher than sequential scans to prefer seq scans when advantageous
- Respects the enable_tidscan GUC parameter
- TID range quals are assumed to be a subset of overall restriction quals
- Uses ceil() to ensure at least one page is always estimated to be accessed