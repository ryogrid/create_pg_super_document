# hypothetical_cume_dist_final

## Location
[src/backend/utils/adt/orderedsetaggs.c:1278-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L1278-L1294)

## Overview
Implements the SQL cumulative distribution function for hypothetical rows in ordered-set aggregates, calculating the cumulative distribution value for where a hypothetical row would appear in a dataset.

## Definition
```c
Datum hypothetical_cume_dist_final(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the final phase of the cume_dist() ordered-set aggregate function for hypothetical rows. It calculates the cumulative distribution of a hypothetical row within an ordered set of data. The cumulative distribution is computed as rank / (total_rows + 1), where rank is the position the hypothetical row would occupy when sorted with peers behind it (using flag +1 in hypothetical_rank_common). This gives a value between 0.0 and 1.0, representing the fraction of rows that would be less than or equal to the hypothetical row.

The key difference from percent_rank is that cume_dist includes equal values in the count and uses (rowcount + 1) as the denominator, which accounts for the hypothetical row itself in the total population.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing the aggregate state and hypothetical row values

## Dependencies
- Functions called/Symbols referenced:
  - [hypothetical_rank_common](hypothetical_rank_common.md): Computes the rank position of the hypothetical row (with flag +1 to sort behind peers)
  - PG_RETURN_FLOAT8: PostgreSQL macro to return a double precision value
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's aggregate function dispatch mechanism)

## Notes and Other Information
- This is part of PostgreSQL's ordered-set aggregate functions implementation
- The cume_dist function is defined in SQL standard and returns values in the range (0,1]
- Uses flag +1 in hypothetical_rank_common to sort the hypothetical row behind its peers, ensuring proper cumulative distribution calculation
- The formula rank/(rowcount+1) ensures that cumulative distribution values are properly distributed across the range
- Used in SQL queries like `SELECT cume_dist(value) WITHIN GROUP (ORDER BY column) FROM table`
- Located in src/backend/utils/adt/orderedsetaggs.c:1278-1294

## Simplified Source
```c
Datum hypothetical_cume_dist_final(PG_FUNCTION_ARGS) {
    int64 rank, rowcount;
    double result_val;

    // Get rank with flag +1 to sort hypothetical row behind peers
    rank = hypothetical_rank_common(fcinfo, 1, &rowcount);

    // Calculate cumulative distribution: rank / (total_rows + 1)
    result_val = (double) rank / (double) (rowcount + 1);

    PG_RETURN_FLOAT8(result_val);
}
```