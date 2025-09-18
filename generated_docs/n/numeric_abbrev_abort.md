# numeric_abbrev_abort

## Location
src/backend/utils/adt/numeric.c: 2124 - 2196

## Overview
Determines whether to abort the numeric abbreviation strategy during sorting by analyzing the cardinality of abbreviated values to ensure abbreviation provides performance benefits.

## Definition


## Detailed Description
The `numeric_abbrev_abort` function implements the adaptive abort logic for PostgreSQL's numeric abbreviation sorting strategy. It monitors the effectiveness of abbreviation by tracking the cardinality of abbreviated values and decides whether to continue or abort the abbreviation process.

The function uses sophisticated heuristics based on statistical analysis:

1. **Early Termination Conditions**: Returns false immediately if there are fewer than 10,000 memory tuples, input count, or if estimation has already been disabled
2. **High Cardinality Optimization**: Stops cardinality estimation (but continues abbreviation) when cardinality exceeds 100,000 distinct values, as abbreviation remains beneficial at this scale
3. **Low Cardinality Abort**: Aborts abbreviation if cardinality falls below the threshold of 1 per ~10,000 non-null inputs, indicating abbreviation overhead exceeds benefits

The target minimum cardinality threshold is conservatively set at 1 per 10k rows (rather than the break-even point of ~1 per 100k) to ensure reliable performance gains and allow early detection of pathological data patterns.

## Parameters / Member Variables
- `memtupcount`: Number of tuples currently in memory during sorting
- `ssup`: SortSupport structure containing abbreviation context and statistics

## Dependencies
- Functions called/Symbols referenced:
  - estimateHyperLogLog (cardinality estimation)
  - elog (optional debug logging when TRACE_SORT enabled)
- Called from (representative examples):
  - [numeric_sortsupport](numeric_sortsupport.md) (as abbreviation abort callback)

## Notes and Other Information
- This is a static function internal to numeric.c module
- Uses HyperLogLog algorithm for efficient cardinality estimation
- Includes comprehensive debug logging when TRACE_SORT is defined
- The abort decision is based purely on abbreviated value cardinality, not original value cardinality
- Conservative threshold (1 per 10k vs 1 per 100k break-even) provides safety margin against pathological cases
- Once cardinality exceeds 100k, estimation overhead is eliminated while continuing abbreviation
- Includes a 0.5 row fudge factor to handle edge cases with exactly one abbreviated value in first 10k rows