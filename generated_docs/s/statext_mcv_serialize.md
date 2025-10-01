# statext_mcv_serialize

## Location
[src/backend/statistics/mcv.c:621-995](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L621-L995)

## Overview
Serializes an MCVList structure into a compact binary format for storage in PostgreSQL's system catalog, with deduplication and space optimization.

## Definition
```c
bytea *statext_mcv_serialize(MCVList *mcvlist, VacAttrStats **stats)
```

## Detailed Description
This function converts an in-memory MCVList structure into a serialized bytea format suitable for storage in pg_statistic_ext_data. The serialization process includes sophisticated optimization techniques: it deduplicates repeated values across different MCV items by creating per-column arrays of unique values and replacing item values with uint16 indexes into these arrays. The function handles various PostgreSQL data types (by-value, fixed-length by-reference, varlena, cstring) with appropriate packing strategies. The resulting format includes header information, dimension metadata, deduplicated value arrays, and MCV items with indexed references.

## Parameters / Member Variables
- `mcvlist`: The in-memory MCVList structure to serialize
- `stats`: Array of VacAttrStats containing type information for each column/dimension

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [PrepareSortSupportFromOrderingOp](../P/PrepareSortSupportFromOrderingOp.md)
  - qsort_interruptible
  - [compare_scalars_simple](../c/compare_scalars_simple.md)
  - [compare_datums_simple](../c/compare_datums_simple.md)
  - PG_DETOAST_DATUM
  - [store_att_byval](store_att_byval.md)
  - [bsearch_arg](../b/bsearch_arg.md)
  - SET_VARSIZE
  - [palloc0](../p/palloc0.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [statext_store](statext_store.md)

## Notes and Other Information
- Uses uint16 indexes to reference deduplicated values, limiting to 65k unique values per column
- Optimizes storage by eliminating redundant values across MCV items
- Handles alignment requirements differently for serialized vs. deserialized formats
- Supports all PostgreSQL data types with specialized handling for varlena and cstring types
- The serialized format includes magic numbers and type information for validation
- Memory allocation is done in chunks to minimize fragmentation
- Part of PostgreSQL's extended statistics system for efficient catalog storage
- The function includes extensive assertions for debugging and validation
- Detoasts varlena values during serialization for consistent storage format

## Simplified Source

```c
bytea *statext_mcv_serialize(MCVList *mcvlist, VacAttrStats **stats) {
    int ndims = mcvlist->ndimensions;
    bytea *raw;
    char *ptr;
    Size total_length;

    // Arrays to store deduplicated values per dimension
    Datum **values = (Datum **) palloc0(sizeof(Datum *) * ndims);
    int *counts = (int *) palloc0(sizeof(int) * ndims);
    DimensionInfo *info = (DimensionInfo *) palloc0(sizeof(DimensionInfo) * ndims);
    SortSupport ssup = (SortSupport) palloc0(sizeof(SortSupportData) * ndims);

    // Phase 1: Collect and deduplicate values for each dimension
    for (int dim = 0; dim < ndims; dim++) {
        // Setup type information and sorting
        info[dim].typlen = stats[dim]->attrtype->typlen;
        info[dim].typbyval = stats[dim]->attrtype->typbyval;
        values[dim] = (Datum *) palloc0(sizeof(Datum) * mcvlist->nitems);

        // Collect non-null values
        for (int i = 0; i < mcvlist->nitems; i++) {
            if (!mcvlist->items[i].isnull[dim]) {
                values[dim][counts[dim]] = mcvlist->items[i].values[dim];
                counts[dim]++;
            }
        }

        if (counts[dim] > 0) {
            // Sort and deduplicate values
            setup_sort_support(&ssup[dim], stats[dim]);
            qsort_interruptible(values[dim], counts[dim], sizeof(Datum),
                              compare_scalars_simple, &ssup[dim]);

            // Remove duplicates, calculate storage size
            int ndistinct = deduplicate_values(values[dim], counts[dim], &ssup[dim]);
            info[dim].nvalues = ndistinct;
            calculate_storage_size(&info[dim], values[dim], ndistinct);
        }
    }

    // Phase 2: Calculate total serialized size
    total_length = calculate_total_size(mcvlist, info, ndims);

    // Phase 3: Serialize to output buffer
    raw = (bytea *) palloc0(VARHDRSZ + total_length);
    SET_VARSIZE(raw, VARHDRSZ + total_length);
    ptr = VARDATA(raw);

    // Store header information
    ptr = copy_mcv_header(ptr, mcvlist, info, ndims);

    // Store deduplicated values for all dimensions
    ptr = copy_deduplicated_values(ptr, values, info, ndims);

    // Store MCV items with indexes instead of values
    ptr = copy_mcv_items_with_indexes(ptr, mcvlist, values, info, ssup, ndims);

    pfree(values);
    pfree(counts);

    return raw;
}
```