# inet_hist_inclusion_join_sel

## Location
[src/backend/utils/adt/network_selfuncs.c:742-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L742-L792)

## Overview
Estimates join selectivity between two histogram distributions for inet network inclusion operators by sampling values from one histogram and measuring their matches against the other.

## Definition

```c
static Selectivity
inet_hist_inclusion_join_sel(Datum *hist1_values, int hist1_nvalues,
							 Datum *hist2_values, int hist2_nvalues,
							 int opr_codenum)
```
## Detailed Description
This function performs selectivity estimation for join operations between two relations that both have histogram statistics for their inet columns. It uses a sampling-based approach to estimate how well the two distributions will match under the specified inclusion operator.

The algorithm works by:
1. Taking interior values from the second histogram (excluding first and last elements as potentially unrepresentative boundary values)
2. Treating these values as a uniform sample of the non-MCV population for that relation
3. For each sampled value, calling inet_hist_value_sel to determine what fraction of the first histogram it matches
4. Averaging the results across all sampled values

For performance optimization, when there are too many histogram elements, the function decimates the second histogram by examining every k-th element where k = (hist2_nvalues - 3) / MAX_CONSIDERED_ELEMS + 1.

The approach is asymmetric (it samples from hist2 and tests against hist1), and the comment notes that using the operator's commutator to do it both ways and average the results might provide more reliable estimates.

## Parameters / Member Variables
- `*hist1_values`: Array of Datum values representing the first histogram's bucket boundaries
- `hist1_nvalues`: Number of values in the first histogram array
- `*hist2_values`: Array of Datum values representing the second histogram's bucket boundaries (source of samples)
- `hist2_nvalues`: Number of values in the second histogram array
- `opr_codenum`: Operator code number indicating the type of inclusion operation
## Dependencies
- Functions called/Symbols referenced:
  - [inet_hist_value_sel](inet_hist_value_sel.md)
  - MAX_CONSIDERED_ELEMS
- Called from (representative examples):
  - [networkjoinsel_inner](../n/networkjoinsel_inner.md)

## Notes and Other Information
- Returns 0.0 when hist2_nvalues <= 2, as there are no interior elements to sample
- Excludes histogram boundary elements (first and last) from sampling as they may not be representative
- The asymmetric approach could potentially be improved by testing both directions and averaging
- Sample decimation helps control runtime when dealing with large histograms
- Designed specifically for inet inclusion operators rather than general comparison operators
- The sampling approach assumes the interior histogram values are reasonably representative of the overall distribution

## Simplified Source

```c
static Selectivity
inet_hist_inclusion_join_sel(Datum *hist1_values, int hist1_nvalues,
                             Datum *hist2_values, int hist2_nvalues,
                             int opr_codenum)
{
    double match = 0.0;
    int i, k, n;

    // Need at least 3 elements (first, middle, last) to have interior values
    if (hist2_nvalues <= 2)
        return 0.0;

    // Decimate hist2 if too large for performance
    k = (hist2_nvalues - 3) / MAX_CONSIDERED_ELEMS + 1;

    n = 0;
    // Sample interior values from hist2 (exclude first and last as outliers)
    for (i = 1; i < hist2_nvalues - 1; i += k)
    {
        // Test each sampled value against hist1 and accumulate matches
        match += inet_hist_value_sel(hist1_values, hist1_nvalues, hist2_values[i], opr_codenum);
        n++;
    }

    // Return average selectivity across sampled values
    return match / n;
}
```