# analyze_mcv_list

## Location
[src/backend/commands/analyze.c:2934-3044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L2934-L3044)

## Overview
Analyzes a list of most common values (MCVs) from a sample to determine how many are statistically significant enough to be stored in the table's MCV statistics.

## Definition
```c
static int analyze_mcv_list(int *mcv_counts, int num_mcv, double stadistinct, 
                           double stanullfrac, int samplerows, double totalrows)
```

## Detailed Description
The `analyze_mcv_list` function implements a statistical analysis algorithm to determine which most common values from a sample are significantly more frequent than they would be if they weren't tracked as MCVs. This is crucial for PostgreSQL's query planner to make accurate cardinality estimates.

The function uses a sophisticated statistical approach based on hypergeometric distribution and confidence intervals. It iteratively removes the least common values from the MCV list if they are not significantly more common than the estimated selectivity they would have as non-MCV values. The algorithm employs a continuity-corrected Wald-type confidence interval with approximately 2 standard errors to determine statistical significance.

The key insight is that all non-MCV values are assumed to be equally common after accounting for MCV frequencies and null values. The function calculates whether each MCV candidate is significantly more frequent than this baseline using hypergeometric variance calculations.

## Parameters / Member Variables
- `mcv_counts`: Array of counts for the most common values, ordered from most to least common
- `num_mcv`: Number of entries in the mcv_counts array
- `stadistinct`: Estimated number of distinct values (negative values indicate fraction of total rows)
- `stanullfrac`: Fraction of null values in the column
- `samplerows`: Number of rows in the analyzed sample
- `totalrows`: Total number of rows in the table

## Dependencies
- Functions called/Symbols referenced:
  - `sqrt` (mathematical function)
- Called from (representative examples):
  - [compute_scalar_stats](../c/compute_scalar_stats.md) (during ANALYZE operation)

## Notes and Other Information
The algorithm deliberately works by removing values from the full list rather than adding them, because the latter approach can fail when common values have similar frequencies and dominate the table. The function uses hypergeometric distribution calculations since sampling is done without replacement. If the entire table was sampled, all MCVs are kept. The statistical confidence interval uses a 2-standard-error threshold plus continuity correction of 0.5 to determine significance.

## Simplified Source
```c
static int analyze_mcv_list(int *mcv_counts, int num_mcv, double stadistinct,
                           double stanullfrac, int samplerows, double totalrows) {
    // If entire table sampled, keep all MCVs
    if (samplerows == totalrows || totalrows <= 1.0)
        return num_mcv;

    // Calculate estimated distinct values in full table
    double ndistinct_table = stadistinct;
    if (ndistinct_table < 0)
        ndistinct_table = -ndistinct_table * totalrows;

    // Track sum of all but least common MCV
    double sumcount = 0.0;
    for (int i = 0; i < num_mcv - 1; i++)
        sumcount += mcv_counts[i];

    // Remove statistically insignificant MCVs from end of list
    while (num_mcv > 0) {
        // Calculate expected selectivity if this value wasn't an MCV
        double selectivity = 1.0 - sumcount / samplerows - stanullfrac;
        if (selectivity < 0.0) selectivity = 0.0;
        if (selectivity > 1.0) selectivity = 1.0;

        double other_distinct = ndistinct_table - (num_mcv - 1);
        if (other_distinct > 1)
            selectivity /= other_distinct;

        // Calculate hypergeometric confidence interval
        double N = totalrows;
        double n = samplerows;
        double K = N * mcv_counts[num_mcv - 1] / n;
        double variance = n * K * (N - K) * (N - n) / (N * N * (N - 1));
        double stddev = sqrt(variance);

        // Test if MCV count is significantly higher than expected
        if (mcv_counts[num_mcv - 1] > selectivity * samplerows + 2 * stddev + 0.5) {
            break;  // Keep this and all more common values
        } else {
            // Remove this value and try next least common
            num_mcv--;
            if (num_mcv == 0) break;
            sumcount -= mcv_counts[num_mcv - 1];
        }
    }

    return num_mcv;
}
```