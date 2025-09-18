# statext_mcv_serialize

## Location
src/backend/statistics/mcv.c: 621 - 995

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
  - lookup_type_cache
  - PrepareSortSupportFromOrderingOp
  - qsort_interruptible
  - compare_scalars_simple
  - compare_datums_simple
  - PG_DETOAST_DATUM
  - store_att_byval
  - bsearch_arg
  - SET_VARSIZE
  - palloc0
  - pfree
- Called from (representative examples):
  - statext_store

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