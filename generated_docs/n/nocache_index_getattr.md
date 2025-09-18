# nocache_index_getattr

## Location
src/backend/access/common/indextuple.c: 241 - 455

## Overview
The `nocache_index_getattr` function extracts a specific attribute value from an IndexTuple when cached offsets are not available, implementing an optimized attribute offset caching strategy.

## Definition
```c
Datum nocache_index_getattr(IndexTuple tup, int attnum, TupleDesc tupleDesc)
```

## Detailed Description
This function is called from the `index_getattr()` macro in cases where cached offsets cannot be used and the requested attribute value is not null. It implements a sophisticated attribute offset caching mechanism to optimize future attribute access operations.

The function handles three main scenarios:
1. **Fast path**: No nulls and no variable-width attributes up to the target attribute
2. **Null handling**: Presence of null values requiring careful navigation through the null bitmap
3. **Variable-width handling**: Variable-length attributes requiring dynamic offset calculation

Key optimizations include:
- Caching attribute offsets in the tuple descriptor for future use
- Bulk calculation of offsets for all leading fixed-width columns
- Careful null bitmap navigation to skip null attributes
- Alignment handling for both fixed-width and variable-length attributes

The caching strategy is designed to perform well for queries that access large numbers of tuples using the same attribute descriptor, as offset calculations are cached and reused across tuples.

## Parameters
- `tup`: IndexTuple from which to extract the attribute value
- `attnum`: 1-based attribute number to extract (gets decremented internally to 0-based)
- `tupleDesc`: TupleDesc describing the tuple structure and providing caching storage

## Dependencies
- Functions called/Symbols referenced:
  - IndexInfoFindDataOffset
  - IndexTupleHasNulls
  - IndexTupleHasVarwidths
  - fetchatt
  - att_isnull
  - att_align_nominal
  - att_align_pointer
  - att_addlength_pointer
- Data types used:
  - bits8 (for null bitmap navigation)
  - IndexTupleData
- Called from:
  - index_getattr macro (src/include/access/itup.h:134, 144)

## Notes and Other Information
- Located in src/backend/access/common/indextuple.c:241-455
- Uses a sophisticated offset caching strategy that stores calculated offsets in the tuple descriptor's attcacheoff field
- Handles three distinct cases based on the presence of nulls and variable-width attributes before the target attribute
- The null bitmap is located immediately after the IndexTupleData header
- For fixed-width columns without preceding nulls or variable-width attributes, the function pre-calculates and caches offsets for all leading columns
- Variable-length attribute handling includes proper alignment considerations
- The caching mechanism improves performance significantly for repeated access to the same attributes across multiple tuples
- Comment indicates this approach was designed by "cim 5/4/91" as a performance optimization