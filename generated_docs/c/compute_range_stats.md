# compute_range_stats

## Location
[src/backend/utils/adt/rangetypes_typanalyze.c:125-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_typanalyze.c#L125-L427)

## Overview
The  function is the core statistics computation routine for range and multirange columns, responsible for analyzing sample data and generating histograms and other statistical information used by the PostgreSQL query planner.

## Definition

```c
struct ranges from first and
			 * last entries in lowers[] and uppers[] along with evenly-spaced
			 * values in between. So the i'th value is a range of lowers[(i *
			 * (nvals - 1)) / (num_hist - 1)] and uppers[(i * (nvals - 1)) /
			 * (num_hist - 1)]. But computing that subscript directly risks
			 * integer overflow when the stats target is more than a couple
			 * thousand.  Instead we add (nvals - 1) / (num_hist - 1) to pos
			 * at each step, tracking the integral and fractional parts of the
			 * sum separately.
			 */
			delta = (non_empty_cnt - 1) / (num_hist - 1);
```
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

## Simplified Source

```c
static void compute_range_stats(VacAttrStats *stats, AnalyzeAttrFetchFunc fetchfunc,
                               int samplerows, double totalrows)
{
    TypeCacheEntry *typcache = (TypeCacheEntry *) stats->extra_data;
    bool has_subdiff = OidIsValid(typcache->rng_subdiff_finfo.fn_oid);

    // Handle multirange types by using underlying range type
    if (typcache->typtype == TYPTYPE_MULTIRANGE)
        typcache = typcache->rngtype;

    int null_cnt = 0, non_null_cnt = 0, non_empty_cnt = 0, empty_cnt = 0;
    int num_bins = stats->attstattarget;

    // Allocate arrays for range bounds and lengths
    RangeBound *lowers = palloc(sizeof(RangeBound) * samplerows);
    RangeBound *uppers = palloc(sizeof(RangeBound) * samplerows);
    float8 *lengths = palloc(sizeof(float8) * samplerows);
    double total_width = 0;

    // Process each sample row
    for (int i = 0; i < samplerows; i++) {
        Datum value;
        bool isnull, empty;

        value = fetchfunc(stats, i, &isnull);
        if (isnull) {
            null_cnt++;
            continue;
        }

        total_width += VARSIZE_ANY(DatumGetPointer(value));

        // Extract range bounds (handles both range and multirange)
        RangeBound lower, upper;
        if (typcache->typtype == TYPTYPE_MULTIRANGE) {
            MultirangeType *multirange = DatumGetMultirangeTypeP(value);
            if (!MultirangeIsEmpty(multirange)) {
                // Get bounds of encompassing range
                multirange_get_bounds(typcache, multirange, 0, &lower, &upper);
                multirange_get_bounds(typcache, multirange,
                                    multirange->rangeCount - 1, &lower, &upper);
                empty = false;
            } else {
                empty = true;
            }
        } else {
            RangeType *range = DatumGetRangeTypeP(value);
            range_deserialize(typcache, range, &lower, &upper, &empty);
        }

        if (!empty) {
            lowers[non_empty_cnt] = lower;
            uppers[non_empty_cnt] = upper;

            // Calculate range length
            float8 length;
            if (lower.infinite || upper.infinite) {
                length = get_float8_infinity();
            } else if (has_subdiff) {
                length = DatumGetFloat8(FunctionCall2Coll(&typcache->rng_subdiff_finfo,
                                                        typcache->rng_collation,
                                                        upper.val, lower.val));
            } else {
                length = 1.0;  // Default length
            }
            lengths[non_empty_cnt] = length;
            non_empty_cnt++;
        } else {
            empty_cnt++;
        }
        non_null_cnt++;
    }

    // Generate statistics if we have data
    if (non_null_cnt > 0) {
        stats->stats_valid = true;
        stats->stanullfrac = (double) null_cnt / samplerows;
        stats->stawidth = total_width / non_null_cnt;
        stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);

        MemoryContext old_cxt = MemoryContextSwitchTo(stats->anl_context);
        int slot_idx = 0;

        // Create bounds histogram if we have enough data
        if (non_empty_cnt >= 2) {
            qsort_interruptible(lowers, non_empty_cnt, sizeof(RangeBound),
                              range_bound_qsort_cmp, typcache);
            qsort_interruptible(uppers, non_empty_cnt, sizeof(RangeBound),
                              range_bound_qsort_cmp, typcache);

            int num_hist = (non_empty_cnt > num_bins) ? num_bins + 1 : non_empty_cnt;
            Datum *bound_hist_values = palloc(num_hist * sizeof(Datum));

            // Create evenly-spaced histogram entries
            for (int i = 0; i < num_hist; i++) {
                int pos = (i * (non_empty_cnt - 1)) / (num_hist - 1);
                bound_hist_values[i] = PointerGetDatum(range_serialize(typcache,
                                                                     &lowers[pos],
                                                                     &uppers[pos],
                                                                     false, NULL));
            }

            stats->stakind[slot_idx] = STATISTIC_KIND_BOUNDS_HISTOGRAM;
            stats->stavalues[slot_idx] = bound_hist_values;
            stats->numvalues[slot_idx] = num_hist;
            stats->statypid[slot_idx] = typcache->type_id;
            slot_idx++;
        }

        // Create length histogram
        if (non_empty_cnt >= 2) {
            qsort_interruptible(lengths, non_empty_cnt, sizeof(float8),
                              float8_qsort_cmp, NULL);

            int num_hist = (non_empty_cnt > num_bins) ? num_bins + 1 : non_empty_cnt;
            Datum *length_hist_values = palloc(num_hist * sizeof(Datum));

            for (int i = 0; i < num_hist; i++) {
                int pos = (i * (non_empty_cnt - 1)) / (num_hist - 1);
                length_hist_values[i] = Float8GetDatum(lengths[pos]);
            }

            stats->stavalues[slot_idx] = length_hist_values;
            stats->numvalues[slot_idx] = num_hist;
        } else {
            stats->stavalues[slot_idx] = palloc(0);
            stats->numvalues[slot_idx] = 0;
        }

        // Store empty fraction
        float4 *emptyfrac = palloc(sizeof(float4));
        *emptyfrac = (double) empty_cnt / non_null_cnt;
        stats->stanumbers[slot_idx] = emptyfrac;
        stats->numnumbers[slot_idx] = 1;
        stats->stakind[slot_idx] = STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM;

        MemoryContextSwitchTo(old_cxt);
    }
}
```