# build_sorted_items

## Location
[src/backend/statistics/extended_stats.c:986-1117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L986-L1117)

## Overview
Builds a sorted array of SortItem structures from sample rows for extended statistics computation, handling memory allocation and data transformation efficiently.

## Definition

```c
SortItem *
build_sorted_items(StatsBuildData *data, int *nitems,
				   MultiSortSupport mss,
				   int numattrs, AttrNumber *attnums)
```
## Detailed Description
This function creates a sorted array of SortItem structures from statistical sample data. It performs several critical operations: allocates memory in a single chunk for efficiency, extracts and processes attribute values from sample rows, handles variable-length data by detoasting when necessary, filters out overly wide values that exceed WIDTH_THRESHOLD, and finally sorts the resulting items using multi-column sort support. The function is designed to support extended statistics calculations like dependency analysis and multi-column value (MCV) statistics.

## Parameters / Member Variables
- : StatsBuildData structure containing sample rows and metadata for statistics computation
- : Output parameter that receives the actual number of items in the resulting sorted array
- : MultiSortSupport structure providing multi-column sorting capabilities
- : Number of attributes to process from the sample data
- : Array of attribute numbers specifying which attributes to include

## Dependencies
- Functions called/Symbols referenced:
  - [get_typlen](../g/get_typlen.md)
  - [toast_raw_datum_size](../t/toast_raw_datum_size.md)
  - PG_DETOAST_DATUM
  - qsort_interruptible
  - [multi_sort_compare](../m/multi_sort_compare.md)
  - WIDTH_THRESHOLD (constant)
  - [StatsBuildData](../S/StatsBuildData.md) (type)
  - MultiSortSupport (type)
  - [SortItem](../S/SortItem.md) (type)
- Called from (representative examples):
  - [dependency_degree](../d/dependency_degree.md)
  - [statext_mcv_build](../s/statext_mcv_build.md)

## Notes and Other Information
- Memory is allocated as a single contiguous chunk for efficient cleanup - caller only needs to pfree() the return value
- Filters out rows containing values that are too wide (exceed WIDTH_THRESHOLD) to avoid memory issues
- Handles variable-length attributes by detoasting them when processing
- Returns NULL if all sample rows are filtered out due to overly wide values
- Includes comprehensive memory layout management with pointer arithmetic to organize SortItem array, Datum values, and null flags
- Uses qsort_interruptible to allow query cancellation during sorting of large datasets
- Located in src/backend/statistics/extended_stats.c:986-1117

## Simplified Source

```c
SortItem *
build_sorted_items(StatsBuildData *data, int *nitems,
                   MultiSortSupport mss,
                   int numattrs, AttrNumber *attnums)
{
    int i, j, len, nrows;
    int nvalues = data->numrows * numattrs;
    SortItem *items;
    Datum *values;
    bool *isnull;
    char *ptr;
    int *typlen;

    // Calculate total memory needed
    len = data->numrows * sizeof(SortItem) +
          nvalues * (sizeof(Datum) + sizeof(bool));

    // Allocate and organize memory layout
    ptr = palloc0(len);
    items = (SortItem *) ptr;
    ptr += data->numrows * sizeof(SortItem);
    values = (Datum *) ptr;
    ptr += nvalues * sizeof(Datum);
    isnull = (bool *) ptr;

    // Set up item pointers
    nrows = 0;
    for (i = 0; i < data->numrows; i++)
    {
        items[nrows].values = &values[nrows * numattrs];
        items[nrows].isnull = &isnull[nrows * numattrs];
        nrows++;
    }

    // Cache type lengths for efficiency
    typlen = (int *) palloc(sizeof(int) * data->nattnums);
    for (i = 0; i < data->nattnums; i++)
        typlen[i] = get_typlen(data->stats[i]->attrtypid);

    // Process sample rows
    nrows = 0;
    for (i = 0; i < data->numrows; i++)
    {
        bool toowide = false;

        // Load values for each attribute
        for (j = 0; j < numattrs; j++)
        {
            Datum value;
            bool isnull_val;
            int attlen;
            AttrNumber attnum = attnums[j];
            int idx;

            // Find attribute index in data
            for (idx = 0; idx < data->nattnums; idx++)
            {
                if (attnum == data->attnums[idx])
                    break;
            }

            value = data->values[idx][i];
            isnull_val = data->nulls[idx][i];
            attlen = typlen[idx];

            // Handle variable-length values
            if ((!isnull_val) && (attlen == -1))
            {
                if (toast_raw_datum_size(value) > WIDTH_THRESHOLD)
                {
                    toowide = true;
                    break;
                }
                value = PointerGetDatum(PG_DETOAST_DATUM(value));
            }

            items[nrows].values[j] = value;
            items[nrows].isnull[j] = isnull_val;
        }

        // Skip rows with values that are too wide
        if (toowide)
            continue;

        nrows++;
    }

    *nitems = nrows;

    // Return NULL if no valid items
    if (nrows == 0)
    {
        pfree(items);
        return NULL;
    }

    // Sort the items
    qsort_interruptible(items, nrows, sizeof(SortItem),
                      multi_sort_compare, mss);

    return items;
}
```