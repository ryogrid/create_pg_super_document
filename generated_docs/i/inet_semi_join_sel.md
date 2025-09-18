# inet_semi_join_sel

## Location
[src/backend/utils/adt/network_selfuncs.c:793-835](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L793-L835)

## Overview
Estimates the probability that at least one row in the right-hand side table satisfies a join condition for a given left-hand side value, specifically designed for semi-join selectivity estimation.

## Definition


## Detailed Description
This function calculates the selectivity for semi-join operations where the goal is to determine if there exists at least one matching row in the right-hand side table for a given left-hand side value. It implements a two-stage approach using both MCV and histogram statistics from the right-hand side table.

The algorithm works as follows:
1. **MCV Check**: If MCV statistics exist, it first checks if the lhs_value matches any of the most common values. If a match is found, it immediately returns 1.0 (certainty of a match).
2. **Histogram Estimation**: If no MCV match is found and histogram statistics exist, it estimates the number of matching rows using inet_hist_value_sel. The operator is commuted (negated) since the function passes lhs_value on the right side.
3. **Probability Calculation**: The histogram selectivity is multiplied by hist_weight (total rows represented by histogram) to get an estimated row count. The result is capped at 1.0 since we only care about existence, not quantity.

The hist_weight parameter represents the total number of rows in the histogram portion of the distribution, excluding MCV and NULL values. This allows for accurate probability scaling.

## Parameters / Member Variables
- : The value from the left-hand side table to test for matches
- : Boolean indicating whether MCV statistics are available
- : Array of most common values from the right-hand side table
- : Number of entries in the MCV array
- : Boolean indicating whether histogram statistics are available
- : Array of histogram bucket boundaries from the right-hand side table
- : Number of values in the histogram array
- : Total number of rows represented by the histogram portion
- : Pre-initialized function manager info for the join operator
- : Operator code number for the join operation

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCall2
  - [DatumGetBool](../D/DatumGetBool.md)
  - [inet_hist_value_sel](inet_hist_value_sel.md)
  - Min
- Called from (representative examples):
  - [networkjoinsel_semi](../n/networkjoinsel_semi.md)

## Notes and Other Information
- Returns 1.0 immediately upon finding any MCV match, optimizing for the most common case
- Commutes the operator by negating opr_codenum when calling inet_hist_value_sel
- Caps the final result at 1.0 since semi-joins only care about existence, not frequency
- Returns 0.0 if neither MCV nor valid histogram statistics provide any matches
- The hist_weight parameter enables accurate scaling from selectivity to probability
- Designed specifically for EXISTS/IN subquery optimization in the PostgreSQL query planner