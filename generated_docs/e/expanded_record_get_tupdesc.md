# expanded_record_get_tupdesc

## Location
[src/include/utils/expandedrecord.h:218-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/expandedrecord.h#L218-L227)

## Overview
Retrieves the tuple descriptor (TupleDesc) for an expanded record, using an optimized fast path when available.

## Definition

```c
static inline TupleDesc
expanded_record_get_tupdesc(ExpandedRecordHeader *erh)
```
## Detailed Description
This inline function provides efficient access to the tuple descriptor of an expanded record. It implements an optimization where if the tuple descriptor is already cached in the ExpandedRecordHeader structure ( field), it returns it immediately using a  hint for branch prediction. If the tuple descriptor is not cached (NULL), it falls back to calling  which will obtain the tuple descriptor from the type cache system and cache it for future use.

The tuple descriptor describes the structure of the composite type represented by the expanded record, including field names, types, and other metadata necessary for record manipulation.

## Parameters / Member Variables
- : Pointer to an ExpandedRecordHeader structure for which to retrieve the tuple descriptor

## Dependencies
- Functions called/Symbols referenced:
  - likely (branch prediction macro)
  - [expanded_record_fetch_tupdesc](expanded_record_fetch_tupdesc.md)
  - ExpandedRecordHeader
- Called from (representative examples):
  - [ExecEvalFieldSelect](../E/ExecEvalFieldSelect.md)
  - [make_expanded_record_from_exprecord](../m/make_expanded_record_from_exprecord.md)
  - [ER_get_flat_size](../E/ER_get_flat_size.md)
  - [ER_flatten_into](../E/ER_flatten_into.md)
  - [deconstruct_expanded_record](../d/deconstruct_expanded_record.md)
  - [expanded_record_lookup_field](expanded_record_lookup_field.md)
  - [build_dummy_expanded_header](../b/build_dummy_expanded_header.md)

## Notes and Other Information
- This is an inline function for maximum performance in hot code paths
- Uses branch prediction optimization with  macro since the tuple descriptor is typically cached
- Part of PostgreSQL's expanded object infrastructure for efficient composite type handling
- The tuple descriptor may be reference-counted from the type cache or locally allocated
- If locally allocated, the ER_FLAG_TUPDESC_ALLOCED flag will be set in the header
- Located in src/include/utils/expandedrecord.h:218-227

## Simplified Source

```c
static inline TupleDesc
expanded_record_get_tupdesc(ExpandedRecordHeader *erh)
{
    // Fast path: return cached tuple descriptor if available
    if (likely(erh->er_tupdesc != NULL))
        return erh->er_tupdesc;

    // Slow path: fetch and cache tuple descriptor
    return expanded_record_fetch_tupdesc(erh);
}
```