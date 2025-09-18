# uuid_abbrev_abort

## Location
[src/backend/utils/adt/uuid.c:292-357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L292-L357)

## Overview
A callback function used to estimate the effectiveness of abbreviated key optimization in UUID sorting operations and decide whether to abort the abbreviation strategy.

## Definition


## Detailed Description
This function is a crucial component of PostgreSQL's sort optimization system for UUID data types. It evaluates the effectiveness of abbreviated key sorting by analyzing the cardinality of abbreviated values versus the total input count. The function uses HyperLogLog estimation to determine if the abbreviated key optimization is providing sufficient benefit to justify its overhead.

The function implements a two-threshold system:
1. **High cardinality threshold**: If more than 100,000 distinct values are detected, abbreviation continues as it's likely beneficial even for very large datasets
2. **Low cardinality threshold**: If cardinality falls below 1 per ~2000 non-null inputs (plus a 0.5 fudge factor), abbreviation is aborted as it's not providing sufficient benefit

## Parameters / Member Variables
- : The number of tuples currently in memory for sorting
- : Sort support structure containing optimization state and callbacks

## Dependencies
- Functions called/Symbols referenced:
  - SortSupport (structure type)
  - uuid_sortsupport_state (structure type)
  - estimateHyperLogLog (cardinality estimation function)
  - TRACE_SORT (conditional compilation macro)
  - INT64_FORMAT (printf format macro)
- Called from (representative examples):
  - [uuid_sortsupport](uuid_sortsupport.md) (as abort callback assignment)

## Notes and Other Information
- The function only begins evaluation after 10,000 tuples and 10,000 input values to ensure statistical significance
- Uses HyperLogLog algorithm for efficient cardinality estimation without storing all distinct values
- Includes detailed trace logging when TRACE_SORT is enabled for debugging sort optimization decisions
- The 0.5 fudge factor in the low cardinality check helps handle pathological cases with very low initial diversity
- Returns false to continue abbreviation, true to abort and switch to full key comparison