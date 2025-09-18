# compute_range_stats

## Location
[src/backend/utils/adt/rangetypes_typanalyze.c:125-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_typanalyze.c#L125-L427)

## Overview
The  function is the core statistics computation routine for range and multirange columns, responsible for analyzing sample data and generating histograms and other statistical information used by the PostgreSQL query planner.

## Definition


## Detailed Description
This function performs comprehensive statistical analysis on range or multirange column data. It processes a sample of the column's data to compute various statistics including:

1. **Basic Statistics**: Null fraction, average width, and distinctness estimates
2. **Bounds Histogram**: A histogram of range boundaries for selectivity estimation
3. **Length Histogram**: A histogram of range lengths for size-based queries  
4. **Empty Range Fraction**: The proportion of ranges that are empty

For multirange types, the function treats each multirange as a single encompassing range from the lowest lower bound to the highest upper bound, effectively analyzing the "convex hull" of the multirange.

The function handles both finite and infinite ranges, uses type-specific subdiff functions when available for length calculations, and creates evenly-distributed histogram bins to provide the query planner with accurate selectivity estimates.

## Parameters / Member Variables
- : VacAttrStats structure to store the computed statistics
- : Function to fetch sample values from the table
- : Number of sample rows to analyze
- : Total number of rows in the table (used for scaling)

## Dependencies
- Functions called/Symbols referenced:
  - ,  (core statistics and range structures)
  - ,  (type classification constants)
  -  (allows interruption during long operations)
  -  (calculates storage size)
  - ,  (multirange handling)
  -  (extracts bounds from multiranges)
  - ,  (range handling)
  -  (infinite length handling)
  - ,  (subdiff function calls)
  -  (sorting range boundaries)
  -  (sorting range lengths)
  -  (creating histogram ranges)
  -  (converting lengths to datums)
  - ,  (histogram types)
- Called from:
  -  (for range columns)
  -  (for multirange columns)

## Notes and Other Information
- Handles both range and multirange types through a unified interface
- Creates two types of histograms: bounds-based for range overlap queries and length-based for size queries
- Uses sophisticated histogram binning that avoids integer overflow for large statistics targets
- Properly handles infinite ranges by assigning them infinite length
- Falls back to default length of 1.0 when no subdiff function is available
- Maintains separate counts for null, empty, and non-empty ranges
- Allocates histogram data in the analyzer's memory context for persistence
- Uses interruptible sorting to allow cancellation during long operations
- The bounds histogram stores actual range values, enabling accurate selectivity estimates for range overlap operations
- The length histogram enables efficient estimation for queries involving range size predicates