# build_column_frequencies

## Location
[src/backend/statistics/mcv.c:490-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L490-L557)

## Overview
Computes frequencies of individual values in each column to support base frequency calculation for MCV (Most Common Values) statistics.

## Definition
```c
static SortItem **build_column_frequencies(SortItem *groups, int ngroups, MultiSortSupport mss, int *ncounts)
```

## Detailed Description
This function analyzes distinct value groups and computes frequency counts for individual column values across all dimensions. It creates arrays of SortItems for each attribute, where each SortItem represents a unique value with its total frequency count. The function optimizes memory usage by allocating all arrays in a single chunk and reusing value/isnull pointers from the input groups. For each column, it sorts values, identifies duplicates, and sums their frequencies to produce accurate per-value statistics used in MCV base frequency calculations.

## Parameters / Member Variables
- `groups`: Array of distinct value groups with their counts
- `ngroups`: Number of distinct groups in the input array
- `mss`: MultiSortSupport structure containing sort specifications for all columns
- `ncounts`: Output array that receives the count of distinct values for each column

## Dependencies
- Functions called/Symbols referenced:
  - [sort_item_compare](../s/sort_item_compare.md)
  - qsort_interruptible
  - [palloc](../p/palloc.md)
  - MAXALIGN (macro)
- Called from (representative examples):
  - SizeOfMCVList
  - [statext_mcv_build](../s/statext_mcv_build.md)

## Notes and Other Information
- Allocates all memory in a single chunk for efficient memory management
- Reuses value/isnull pointers from input groups to avoid data duplication
- Processes each column dimension independently for multi-column statistics
- Sorts and deduplicates values to compute accurate frequency counts
- Essential component for calculating base frequencies in MCV list generation
- Memory can be freed with a single pfree call due to chunk allocation strategy

## Simplified Source

```c
static SortItem **
build_column_frequencies(SortItem *groups, int ngroups,
                        MultiSortSupport mss, int *ncounts)
{
    int i, dim;
    SortItem **result;
    char *ptr;

    // Allocate arrays for all columns as a single memory chunk
    ptr = palloc(MAXALIGN(sizeof(SortItem *) * mss->ndims) +
                 mss->ndims * MAXALIGN(sizeof(SortItem) * ngroups));

    // Set up result array pointers
    result = (SortItem **) ptr;
    ptr += MAXALIGN(sizeof(SortItem *) * mss->ndims);

    // Process each column dimension
    for (dim = 0; dim < mss->ndims; dim++)
    {
        SortSupport ssup = &mss->ssup[dim];

        // Set up array for this column
        result[dim] = (SortItem *) ptr;
        ptr += MAXALIGN(sizeof(SortItem) * ngroups);

        // Copy data from input groups
        for (i = 0; i < ngroups; i++)
        {
            result[dim][i].values = &groups[i].values[dim];
            result[dim][i].isnull = &groups[i].isnull[dim];
            result[dim][i].count = groups[i].count;
        }

        // Sort values for this dimension
        qsort_interruptible(result[dim], ngroups, sizeof(SortItem),
                          sort_item_compare, ssup);

        // Count distinct values and sum frequencies
        ncounts[dim] = 1;
        for (i = 1; i < ngroups; i++)
        {
            if (sort_item_compare(&result[dim][i - 1], &result[dim][i], ssup) == 0)
            {
                // Same value - add to frequency count
                result[dim][ncounts[dim] - 1].count += result[dim][i].count;
                continue;
            }

            // Different value - start new entry
            result[dim][ncounts[dim]] = result[dim][i];
            ncounts[dim]++;
        }
    }

    return result;
}
```