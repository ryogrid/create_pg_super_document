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

## Simplified Source

```c
static void compute_array_stats(VacAttrStats *stats, AnalyzeAttrFetchFunc fetchfunc,
                               int samplerows, double totalrows) {
    ArrayAnalyzeExtraData *extra_data = (ArrayAnalyzeExtraData *) stats->extra_data;

    // First compute standard scalar statistics
    stats->extra_data = extra_data->std_extra_data;
    extra_data->std_compute_stats(stats, fetchfunc, samplerows, totalrows);
    stats->extra_data = extra_data;

    // Set up parameters for Lossy Counting algorithm
    int num_mcelem = stats->attstattarget * 10;
    int bucket_width = num_mcelem * 1000 / 7;  // Based on epsilon = s/10

    // Create hashtables for element tracking and distinct element counts
    HTAB *elements_tab = hash_create("Analyzed elements table", num_mcelem, ...);
    HTAB *count_tab = hash_create("Array distinct element count table", 64, ...);

    int b_current = 1;  // Current batch number
    int64 element_no = 0;  // Total elements processed
    int analyzed_rows = 0;
    int null_elem_cnt = 0;

    // Process each sample array
    for (int array_no = 0; array_no < samplerows; array_no++) {
        Datum value = fetchfunc(stats, array_no, &isnull);
        if (isnull || toast_raw_datum_size(value) > ARRAY_WIDTH_THRESHOLD)
            continue;

        analyzed_rows++;
        ArrayType *array = DatumGetArrayTypeP(value);

        // Deconstruct array into individual elements
        deconstruct_array(array, extra_data->type_id, extra_data->typlen,
                         extra_data->typbyval, extra_data->typalign,
                         &elem_values, &elem_nulls, &num_elems);

        bool null_present = false;
        int64 prev_element_no = element_no;

        // Process each element using Lossy Counting
        for (int j = 0; j < num_elems; j++) {
            if (elem_nulls[j]) {
                null_present = true;
                continue;
            }

            // Find or create element tracking record
            TrackItem *item = (TrackItem *) hash_search(elements_tab,
                                                       &elem_values[j], HASH_ENTER, &found);

            if (found) {
                // Count each distinct element only once per array
                if (item->last_container != array_no) {
                    item->frequency++;
                    item->last_container = array_no;
                }
            } else {
                // Initialize new element
                item->key = datumCopy(elem_values[j], extra_data->typbyval, extra_data->typlen);
                item->frequency = 1;
                item->delta = b_current - 1;
                item->last_container = array_no;
            }

            element_no++;

            // Prune hashtable periodically (Lossy Counting pruning)
            if (element_no % bucket_width == 0) {
                prune_element_hashtable(elements_tab, b_current);
                b_current++;
            }
        }

        // Track null presence and distinct element count for this array
        if (null_present) null_elem_cnt++;

        int distinct_count = (int)(element_no - prev_element_no);
        // Update distinct element count histogram...

        // Cleanup
        if (PointerGetDatum(array) != value) pfree(array);
        pfree(elem_values);
        pfree(elem_nulls);
    }

    // Generate statistics from collected data
    if (analyzed_rows > 0) {
        // Find elements meeting cutoff frequency
        int64 cutoff_freq = 9 * element_no / bucket_width;

        // Create MCELEM statistics (most common elements)
        // Sort by element value for binary search optimization
        // Store frequencies as fraction of non-null arrays

        // Create DECHIST statistics (distinct element count histogram)
        // Build histogram showing distribution of distinct counts per array
    }
}
```