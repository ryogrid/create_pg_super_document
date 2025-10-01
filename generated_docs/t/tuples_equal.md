# tuples_equal

## Location
[src/backend/executor/execReplication.c:305-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L305-L377)

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
  - [slot_getallattrs](../s/slot_getallattrs.md) (to extract all attribute values from slots)
  - TupleDescAttr (to access attribute metadata)
  - [lookup_type_cache](../l/lookup_type_cache.md) (to get type-specific equality operator information)
  - TYPECACHE_EQ_OPR_FINFO (flag for caching equality operator function info)
  - [format_type_be](../f/format_type_be.md) (for error message formatting)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (to invoke equality operator with collation)
  - [DatumGetBool](../D/DatumGetBool.md) (to convert result to boolean)

- Called from (representative examples):
  - [RelationFindReplTupleByIndex](../R/RelationFindReplTupleByIndex.md)
  - [RelationFindReplTupleSeq](../R/RelationFindReplTupleSeq.md)

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

## Simplified Source

```c
static bool
tuples_equal(TupleTableSlot *slot1, TupleTableSlot *slot2, TypeCacheEntry **eq)
{
    int attrnum;

    Assert(slot1->tts_tupleDescriptor->natts == slot2->tts_tupleDescriptor->natts);

    // Extract all attributes from both slots
    slot_getallattrs(slot1);
    slot_getallattrs(slot2);

    // Compare each attribute
    for (attrnum = 0; attrnum < slot1->tts_tupleDescriptor->natts; attrnum++) {
        Form_pg_attribute att;
        TypeCacheEntry *typentry;

        att = TupleDescAttr(slot1->tts_tupleDescriptor, attrnum);

        // Skip dropped and generated columns (not sent by publisher)
        if (att->attisdropped || att->attgenerated)
            continue;

        // Handle NULL values: both NULL = equal, one NULL ≠ non-NULL
        if (slot1->tts_isnull[attrnum] != slot2->tts_isnull[attrnum])
            return false;
        if (slot1->tts_isnull[attrnum] || slot2->tts_isnull[attrnum])
            continue;

        // Get or lookup equality operator for this type
        typentry = eq[attrnum];
        if (typentry == NULL) {
            typentry = lookup_type_cache(att->atttypid, TYPECACHE_EQ_OPR_FINFO);
            if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                               errmsg("could not identify an equality operator for type %s",
                                      format_type_be(att->atttypid))));
            eq[attrnum] = typentry;
        }

        // Compare values using cached equality operator
        if (!DatumGetBool(FunctionCall2Coll(&typentry->eq_opr_finfo,
                                          att->attcollation,
                                          slot1->tts_values[attrnum],
                                          slot2->tts_values[attrnum])))
            return false;
    }

    return true;
}
```