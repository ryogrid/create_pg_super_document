# tuples_equal

## Location
src/backend/executor/execReplication.c: 305 - 377

## Overview
Compares two tuples stored in TupleTableSlots for equality by checking all non-dropped, non-generated attributes using appropriate data type equality operators.

## Definition
```c
static bool tuples_equal(TupleTableSlot *slot1, TupleTableSlot *slot2, TypeCacheEntry **eq)
```

## Detailed Description
This function performs a comprehensive equality comparison between two tuples, specifically designed for replication scenarios where precise tuple matching is required. It handles the complexities of PostgreSQL's type system by using cached equality operators for each data type.

The function works by:
1. **Validation**: Ensures both slots have the same number of attributes
2. **Attribute Extraction**: Forces extraction of all attributes from both slots using `slot_getallattrs()`
3. **Per-Attribute Comparison**: Iterates through each attribute, applying appropriate handling:
   - Skips dropped and generated columns (not sent by publishers in replication)
   - Handles NULL values correctly (both NULL = equal, one NULL ≠ non-NULL)
   - Uses cached type-specific equality operators for efficient comparison
4. **Type Operator Caching**: Maintains a cache of equality operators (`TypeCacheEntry` array) to avoid repeated operator lookups
5. **Equality Testing**: Uses `FunctionCall2Coll()` to invoke the appropriate equality operator with proper collation

The function is optimized for replication workloads where the same tuple structure is compared repeatedly, making the operator caching particularly beneficial for performance.

## Parameters / Member Variables
- `slot1`: First TupleTableSlot containing a tuple to compare
- `slot2`: Second TupleTableSlot containing a tuple to compare  
- `eq`: Array of TypeCacheEntry pointers for caching equality operators (one per attribute)

## Dependencies
- Functions called/Symbols referenced:
  - slot_getallattrs (to extract all attribute values from slots)
  - TupleDescAttr (to access attribute metadata)
  - lookup_type_cache (to get type-specific equality operator information)
  - TYPECACHE_EQ_OPR_FINFO (flag for caching equality operator function info)
  - format_type_be (for error message formatting)
  - FunctionCall2Coll (to invoke equality operator with collation)
  - DatumGetBool (to convert result to boolean)

- Called from (representative examples):
  - RelationFindReplTupleByIndex
  - RelationFindReplTupleSeq

## Notes and Other Information
- This is a static function only accessible within execReplication.c
- Requires that both slots have identical tuple descriptors (same number of attributes)
- Skips dropped columns since they are not transmitted in logical replication
- Skips generated columns as publishers don't send these values
- Uses proper collation-aware comparisons for text types
- Caches equality operators in the provided array to avoid repeated lookups
- Returns false immediately upon finding any non-equal attribute
- Handles NULL values according to SQL semantics (NULL = NULL is true)
- Will error if no equality operator exists for a given data type