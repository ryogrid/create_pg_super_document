# compute_array_stats

## Location
[src/backend/utils/adt/array_typanalyze.c:216-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_typanalyze.c#L216-L680)

## Overview
The compute_array_stats function computes specialized statistics for array columns to support efficient selectivity estimation for array operators like <@, &&, and @>.

## Definition
```c
static void compute_array_stats(VacAttrStats *stats, AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows)
```

## Detailed Description
This function implements the Lossy Counting algorithm to compute statistics for array data types during ANALYZE operations. It first invokes standard scalar statistics computation, then performs specialized array analysis to identify the most common array elements (MCELEM) and create a histogram of distinct element counts (DECHIST). The function uses a hash table to track element frequencies across arrays, counting each distinct element only once per array (since array operators ignore duplicates). The algorithm processes arrays in batches, pruning low-frequency elements periodically to maintain manageable memory usage. The resulting statistics are stored in pg_statistic to support query optimization for array containment and overlap operators.

## Parameters / Member Variables
- `stats`: VacAttrStats structure containing column analysis configuration and results storage
- `fetchfunc`: Function pointer to retrieve sample array values for analysis
- `samplerows`: Number of sample rows to analyze
- `totalrows`: Total number of rows in the table (for statistical calculations)

## Dependencies
- Functions called/Symbols referenced:
  - std_compute_stats (via extra_data)
  - [element_hash](../e/element_hash.md)
  - [element_match](../e/element_match.md)
  - [prune_element_hashtable](../p/prune_element_hashtable.md)
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [deconstruct_array](../d/deconstruct_array.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [array_typanalyze](../a/array_typanalyze.md) (sets this as compute_stats callback)

## Notes and Other Information
- Uses Lossy Counting algorithm with parameters s = 0.07/K, epsilon = s/10 where K is statistics_target * 10
- Skips arrays larger than ARRAY_WIDTH_THRESHOLD to avoid excessive memory usage
- Stores MCELEM statistics with element values, frequencies, and min/max/null frequencies
- Creates DECHIST histogram showing distribution of distinct element counts per array
- Requires element type to support equality, comparison, and hash operations
- Memory management uses temporary hash tables that are automatically cleaned up
- Located in src/backend/utils/adt/array_typanalyze.c:216-680