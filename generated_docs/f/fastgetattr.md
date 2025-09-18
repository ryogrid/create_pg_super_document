# fastgetattr

## Location
src/include/access/htup_details.h: 754 - 796

## Overview
`fastgetattr` is an optimized inline function that extracts attribute values from heap tuples when the attribute number is known to be valid and non-system.

## Definition
```c
static inline Datum
fastgetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
```

## Detailed Description
`fastgetattr` provides a performance-optimized path for retrieving user attribute values from heap tuples. It is designed for scenarios where the caller knows the attribute number is valid and refers to a user attribute (not a system attribute). The function implements two key optimizations:

1. **Cached offset optimization**: For tuples without NULL values and attributes with cached offsets, it uses `fetchatt` to directly access the attribute data at the precomputed offset.
2. **NULL bitmap checking**: For tuples with potential NULL values, it first checks the NULL bitmap before falling back to `nocachegetattr`.

The function is implemented as a static inline to minimize function call overhead, making it suitable for high-frequency operations in PostgreSQL's tuple processing pipeline.

## Parameters / Member Variables
- `tup`: Pointer to the heap tuple containing the data
- `attnum`: The 1-based attribute number to retrieve (must be > 0 for user attributes)
- `tupleDesc`: Tuple descriptor containing metadata about the tuple structure
- `isnull`: Output parameter set to true if the attribute value is NULL, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - `HeapTupleNoNulls` - macro to check if tuple has any NULL values
  - `TupleDescAttr` - macro to access attribute metadata from tuple descriptor
  - `fetchatt` - function to extract attribute value at known offset
  - [nocachegetattr](../n/nocachegetattr.md) - fallback function for uncached attribute retrieval
  - `att_isnull` - function to check if specific attribute is NULL in bitmap
- Called from (representative examples):
  - [heap_getattr](../h/heap_getattr.md) - general attribute access function
  - [CatalogCacheComputeTupleHashValue](../C/CatalogCacheComputeTupleHashValue.md) - catalog cache hash computation
  - [RelationInitIndexAccessInfo](../R/RelationInitIndexAccessInfo.md) - index access info initialization
  - [extractRelOptions](../e/extractRelOptions.md) - relation option extraction

## Notes and Other Information
- **Performance critical**: This function is called frequently throughout PostgreSQL, hence the inline optimization
- **Safety requirements**: The caller must ensure `attnum` is valid (> 0) and refers to a user attribute, not a system attribute
- **NULL handling**: Always sets `*isnull` to indicate whether the returned value is NULL
- **Caching strategy**: Leverages cached attribute offsets when available to avoid recomputation
- **Fallback mechanism**: Uses `nocachegetattr` when cached offsets are not available or when dealing with NULL values