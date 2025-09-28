# fastgetattr

## Location
[src/include/access/htup_details.h:754-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/htup_details.h#L754-L796)

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
  - `[att_isnull](../a/att_isnull.md)` - function to check if specific attribute is NULL in bitmap
- Called from (representative examples):
  - [heap_getattr](../h/heap_getattr.md) - general attribute access function
  - [CatalogCacheComputeTupleHashValue](../C/CatalogCacheComputeTupleHashValue.md) - catalog cache hash computation
  - [RelationInitIndexAccessInfo](../R/RelationInitIndexAccessInfo.md) - [index](../i/index.md) access info initialization
  - [extractRelOptions](../e/extractRelOptions.md) - relation option extraction

## Notes and Other Information
- **Performance critical**: This function is called frequently throughout PostgreSQL, hence the inline optimization
- **Safety requirements**: The caller must ensure `attnum` is valid (> 0) and refers to a user attribute, not a system attribute
- **NULL handling**: Always sets `*isnull` to indicate whether the returned value is NULL
- **Caching strategy**: Leverages cached attribute offsets when available to avoid recomputation
- **Fallback mechanism**: Uses `nocachegetattr` when cached offsets are not available or when dealing with NULL values

## Simplified Source

```c
// Simplified version of fastgetattr
static inline Datum fastgetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull) {
    Assert(attnum > 0);

    *isnull = false;

    // Fast path for tuples with no NULL values
    if (HeapTupleNoNulls(tup)) {
        Form_pg_attribute att = TupleDescAttr(tupleDesc, attnum - 1);

        // Use cached offset if available
        if (att->attcacheoff >= 0)
            return fetchatt(att, (char *) tup->t_data + tup->t_data->t_hoff +
                           att->attcacheoff);
        else
            return nocachegetattr(tup, attnum, tupleDesc);
    } else {
        // Check NULL bitmap first
        if (att_isnull(attnum - 1, tup->t_data->t_bits)) {
            *isnull = true;
            return (Datum) NULL;
        } else {
            return nocachegetattr(tup, attnum, tupleDesc);
        }
    }
}
```

Key simplifications made:
- Preserved the essential attribute extraction optimization logic
- Maintained the fast path for non-NULL tuples with cached offsets
- Kept the NULL bitmap checking for tuples with potential NULLs
- Focused on the core performance optimization strategy
- Retained proper delegation to nocachegetattr fallback