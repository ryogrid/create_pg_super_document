# index_getattr

## Location
[src/include/access/itup.h:118-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/itup.h#L118-L165)

## Overview
Extracts the value of a specific attribute from an IndexTuple, with optimized fast paths for cached offsets and null value checks before falling back to the uncached extraction function.

## Definition
```c
static inline Datum index_getattr(IndexTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
```

## Detailed Description
This high-performance inline function retrieves attribute values from IndexTuple structures with multiple optimization layers. It implements a three-tier approach to minimize overhead for the most common cases:

1. **Fast Path (Cached, No Nulls)**: When the tuple has no null values and the attribute has a cached offset, it directly calculates the attribute location using fetchatt() with the cached offset information.

2. **Null Check Path**: When the tuple contains nulls, it first checks the null bitmap to determine if the requested attribute is null before attempting value extraction.

3. **Fallback Path**: For uncached attributes or complex cases, it delegates to nocache_index_getattr() which handles the full attribute extraction logic.

The function is designed to be called frequently during index operations, hence the macro-like optimizations for the most common scenarios (cacheable lookups and null checks) while maintaining full functionality through the nocache fallback.

## Parameters / Member Variables
- `tup`: IndexTuple pointer to the index tuple containing the desired attribute
- `attnum`: 1-based attribute number (must be > 0) identifying which attribute to extract  
- `tupleDesc`: TupleDesc describing the structure and types of attributes in the tuple
- `isnull`: Pointer to bool that will be set to indicate whether the extracted value is null

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (validation macro)
  - IndexTupleHasNulls (macro)
  - TupleDescAttr (accessor macro)
  - fetchatt (attribute extraction function)
  - [IndexInfoFindDataOffset](../I/IndexInfoFindDataOffset.md) (offset calculation function)
  - [nocache_index_getattr](../n/nocache_index_getattr.md) (fallback extraction function)
  - [att_isnull](../a/att_isnull.md) (null bitmap check function)
  - [IndexTupleData](../I/IndexTupleData.md) (struct type)
  - bits8 (type)
- Called from (representative examples):
  - [gintuple_get_attrnum](../g/gintuple_get_attrnum.md) (GIN index operations)
  - [gintuple_get_key](../g/gintuple_get_key.md) (GIN key extraction)
  - [gistindex_keytest](../g/gistindex_keytest.md) (GiST index testing)
  - [_hash_checkqual](../h/_hash_checkqual.md) (Hash index qualification)
  - [_bt_compare](../b/_bt_compare.md) (B-tree comparison operations)
  - [tuplesort_putindextuplevalues](../t/tuplesort_putindextuplevalues.md) (sorting operations)

## Notes and Other Information
- This is a static inline function providing maximum performance for frequent index attribute access
- The function includes Assert() calls to validate input parameters in debug builds
- The optimization strategy prioritizes the most common case: non-null tuples with cached attribute offsets
- When nulls are present, the null bitmap immediately follows the IndexTupleData header
- The cached offset optimization (attcacheoff >= 0) avoids expensive offset recalculation for fixed-length attribute types
- Falls back to nocache_index_getattr for variable-length attributes or when cache information is not available
- Used extensively throughout PostgreSQL's index access methods (B-tree, Hash, GiST, GIN)