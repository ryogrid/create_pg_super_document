# hypothetical_percent_rank_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:1258-1277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L1258-L1277)

## Overview
Implements the SQL percentile rank function for hypothetical rows in ordered-set aggregates, calculating where a hypothetical row would rank percentile-wise within a dataset.

## Definition

```c
Datum
hypothetical_percent_rank_final(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the final phase of the percent_rank() ordered-set aggregate function for hypothetical rows. It calculates the percentile rank of a hypothetical row within an ordered set of data. The percentile rank is computed as (rank - 1) / total_rows, where rank is the 1-based position the hypothetical row would occupy if inserted into the sorted dataset. This gives a value between 0.0 and 1.0, representing the percentage of rows that would be less than the hypothetical row.

The function handles the special case where there are no regular rows by returning 0.0, indicating that the hypothetical row would be at the 0th percentile (no rows would be less than it).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Function call information structure containing the aggregate state and hypothetical row values
## Dependencies
- Functions called/Symbols referenced:
  - [hypothetical_rank_common](hypothetical_rank_common.md): Computes the rank position of the hypothetical row
  - PG_RETURN_FLOAT8: PostgreSQL macro to return a double precision value
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's aggregate function dispatch mechanism)

## Notes and Other Information
- This is part of PostgreSQL's ordered-set aggregate functions implementation
- The percent_rank function is defined in SQL standard and returns values in the range [0,1]
- The calculation formula (rank-1)/rowcount ensures the first-ranked item gets percentile 0.0 and creates proper distribution
- Used in SQL queries like 
- Located in src/backend/utils/adt/orderedsetaggs.c:1258-1277