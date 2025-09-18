# estimate_ndistinct

## Location
[src/backend/statistics/mvdistinct.c:521-549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L521-L549)

## Overview
A static function that implements the Duj1 estimator algorithm to estimate the number of distinct values from sample data, used by PostgreSQL's statistics collection system.

## Definition


## Detailed Description
This function implements the Duj1 estimator, which is the same n-distinct estimation algorithm used in PostgreSQL's ANALYZE command (analyze.c). The estimator calculates the likely number of distinct values in the full dataset based on sample statistics using the formula:

**ndistinct = (numrows * d) / (numrows - f1 + f1 * numrows / totalrows)**

Where:
- numrows = size of the sample
- d = number of distinct values found in the sample  
- f1 = number of values that appear exactly once in the sample
- totalrows = total number of rows in the table

The function includes safeguards to clamp the result within reasonable bounds (between d and totalrows) to handle potential roundoff errors, and rounds the final result to the nearest integer using floor(value + 0.5).

## Parameters / Member Variables
- : Total number of rows in the complete dataset
- : Number of rows in the sample data
- : Number of distinct values observed in the sample
- : Number of values that appear exactly once in the sample (singleton count)

## Dependencies
- Functions called/Symbols referenced:
  - floor (standard C math function for rounding down)
- Called from:
  - [ndistinct_for_combination](../n/ndistinct_for_combination.md) (estimates distinct values for column combinations)

## Notes and Other Information
- This is the standard Duj1 estimator used throughout PostgreSQL for n-distinct calculations
- The algorithm accounts for the probability that singleton values in the sample represent many more distinct values in the full population
- Results are clamped between d (minimum possible) and totalrows (maximum possible) to prevent estimation errors
- The final result is rounded to the nearest integer for practical use
- Located in src/backend/statistics/mvdistinct.c:521-549
- This is a static function only used within the mvdistinct.c module