# statext_mcv_build

## Location
[src/backend/statistics/mcv.c:180-346](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L180-L346)

## Overview
Builds a Most Common Values (MCV) list from sampled rows for multi-column extended statistics, implementing a four-step algorithm to identify and store the most frequently occurring value combinations.

## Definition
```c
MCVList *statext_mcv_build(StatsBuildData *data, double totalrows, int stattarget)
```

## Detailed Description
This function constructs an MCV list for multi-column statistics using a sophisticated algorithm that differs from single-column MCV construction. The process involves four main steps:

1. **Sort the data**: Uses default collation and '<' operator for the data types
2. **Count distinct groups**: Determines how many distinct value combinations exist
3. **Build MCV list**: Uses a statistical threshold to decide which combinations to keep
4. **Cleanup**: Removes rows represented by the MCV from the sample

The key difference from single-column MCV lists is that this function considers how actual frequencies differ from base frequencies (assuming column independence). It uses `get_mincount_for_mcv_list()` to establish a statistical threshold for inclusion, keeping all groups that appear more frequently than this minimum count.

For each MCV item, the function calculates both the observed frequency and the base frequency (what the frequency would be if columns were independent), enabling detection of both common and unexpectedly rare combinations.

## Parameters / Member Variables
- `data`: Statistical build data containing sampled rows and column information
- `totalrows`: Total number of rows in the table (for statistical calculations)
- `stattarget`: Target number of statistics items to keep (upper bound)

## Dependencies
- Functions called/Symbols referenced:
  - [build_mss](../b/build_mss.md)
  - [build_sorted_items](../b/build_sorted_items.md)
  - [build_distinct_groups](../b/build_distinct_groups.md)
  - [get_mincount_for_mcv_list](../g/get_mincount_for_mcv_list.md)
  - [build_column_frequencies](../b/build_column_frequencies.md)
  - [bsearch_arg](../b/bsearch_arg.md)
  - [multi_sort_compare](../m/multi_sort_compare.md)
  - [MCVList](../M/MCVList.md), MCVItem, SortItem, StatsBuildData
  - STATS_MCV_MAGIC, STATS_MCV_TYPE_BASIC
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)

## Notes and Other Information
- Returns NULL if no items are available or if sorting fails
- May return NULL for uniform distributions with many groups where no values exceed the minimum threshold
- Stores both frequency and base_frequency for each MCV item to enable independence analysis
- Groups are sorted by frequency in descending order
- Uses binary search to efficiently find column frequencies when calculating base frequencies
- Allocates memory for MCVList structure and individual MCVItem components
- The algorithm specifically handles multi-column scenarios where traditional single-column approaches are insufficient

## Simplified Source

```c
MCVList *
statext_mcv_build(StatsBuildData *data, double totalrows, int stattarget)
{
    int numattrs = data->nattnums;
    int numrows = data->numrows;
    int nitems, ngroups;
    double mincount;
    SortItem *items, *groups;
    MCVList *mcvlist = NULL;
    MultiSortSupport mss;

    // Step 1: Build comparator and sort the rows
    mss = build_mss(data);
    items = build_sorted_items(data, &nitems, mss, numattrs, data->attnums);

    if (!items)
        return NULL;

    // Step 2: Transform sorted rows into distinct groups (sorted by frequency)
    groups = build_distinct_groups(nitems, items, mss, &ngroups);

    // Step 3: Determine how many items to keep
    nitems = stattarget;
    if (nitems > ngroups)
        nitems = ngroups;

    // Calculate minimum count threshold
    mincount = get_mincount_for_mcv_list(numrows, totalrows);

    // Find first group below threshold
    for (int i = 0; i < nitems; i++)
    {
        if (groups[i].count < mincount)
        {
            nitems = i;
            break;
        }
    }

    // Step 4: Build MCV list if we have items to keep
    if (nitems > 0)
    {
        SortItem **freqs;
        int *nfreqs;

        // Compute frequencies for values in each column
        nfreqs = palloc0(sizeof(int) * numattrs);
        freqs = build_column_frequencies(groups, ngroups, mss, nfreqs);

        // Allocate MCV list structure
        mcvlist = palloc0(offsetof(MCVList, items) +
                         sizeof(MCVItem) * nitems);

        mcvlist->magic = STATS_MCV_MAGIC;
        mcvlist->type = STATS_MCV_TYPE_BASIC;
        mcvlist->ndimensions = numattrs;
        mcvlist->nitems = nitems;

        // Store data type OIDs
        for (int i = 0; i < numattrs; i++)
            mcvlist->types[i] = data->stats[i]->attrtypid;

        // Copy groups into result
        for (int i = 0; i < nitems; i++)
        {
            MCVItem *item = &mcvlist->items[i];

            // Allocate and copy values
            item->values = palloc(sizeof(Datum) * numattrs);
            item->isnull = palloc(sizeof(bool) * numattrs);
            memcpy(item->values, groups[i].values, sizeof(Datum) * numattrs);
            memcpy(item->isnull, groups[i].isnull, sizeof(bool) * numattrs);

            // Calculate actual frequency
            item->frequency = (double) groups[i].count / numrows;

            // Calculate base frequency (assuming independence)
            item->base_frequency = 1.0;
            for (int j = 0; j < numattrs; j++)
            {
                SortItem key = {&groups[i].values[j], &groups[i].isnull[j]};
                SortItem *freq = bsearch_arg(&key, freqs[j], nfreqs[j],
                                           sizeof(SortItem), multi_sort_compare, mss);
                item->base_frequency *= ((double) freq->count) / numrows;
            }
        }

        pfree(nfreqs);
        pfree(freqs);
    }

    pfree(items);
    pfree(groups);
    return mcvlist;
}
```