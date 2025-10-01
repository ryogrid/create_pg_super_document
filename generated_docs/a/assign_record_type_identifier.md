# assign_record_type_identifier

## Location
[src/backend/utils/cache/typcache.c:2045-2085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2045-L2085)

## Overview
Assigns a unique identifier for the lifetime of the backend process to track the current tuple descriptor of a composite type, with different behavior for named types versus anonymous RECORD types.

## Definition

```c
uint64
assign_record_type_identifier(Oid type_id, int32 typmod)
```
## Detailed Description
This function provides a backend-lifetime unique identification system for tuple descriptors of composite types. It handles two distinct cases with different identity semantics:

For named composite types (type_id != RECORDOID), it retrieves the tuple descriptor from the type cache and returns its persistent identifier. This identifier is guaranteed to change if the type's definition changes, enabling detection of type definition modifications.

For RECORD types, the behavior depends on the typmod value. Registered RECORD types (valid typmod) return a stable identifier from the RecordCacheArray that won't change once assigned. Anonymous RECORD types (typmod < 0 or unrecognized) receive a new identifier on each call, reflecting their transient nature.

The function serves PostgreSQL's expanded record infrastructure by providing a way to detect when cached expanded records become invalid due to type definition changes.

## Parameters / Member Variables
- : OID of the composite type (RECORDOID for record types, or a named composite type OID)
- : Type modifier specifying the particular variant for RECORD types, or -1 for anonymous records

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - TYPECACHE_TUPDESC (flag constant)
- Called from (representative examples):
  - [make_expanded_record_from_typeid](../m/make_expanded_record_from_typeid.md) (src/backend/utils/adt/expandedrecord.c:110)
  - [make_expanded_record_from_tupdesc](../m/make_expanded_record_from_tupdesc.md) (src/backend/utils/adt/expandedrecord.c:242)
  - [expanded_record_fetch_tupdesc](../e/expanded_record_fetch_tupdesc.md) (src/backend/utils/adt/expandedrecord.c:866)

## Notes and Other Information
- Returns backend-lifetime unique uint64 identifiers via tupledesc_id_counter increment
- Named composite type identifiers change when type definitions are modified, enabling cache invalidation
- Registered RECORD type identifiers remain stable once assigned
- Anonymous RECORD types always receive new identifiers, reflecting their ephemeral nature
- Throws ERRCODE_WRONG_OBJECT_TYPE error if a non-composite type OID is provided
- Part of PostgreSQL's expanded record system for efficient record type handling
- Identifiers are guaranteed unique only within the current backend process lifetime

## Simplified Source

```c
uint64 assign_record_type_identifier(Oid type_id, int32 typmod) {
    if (type_id != RECORDOID) {
        // Named composite type - use type cache
        TypeCacheEntry *typentry;

        typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC);
        if (typentry->tupDesc == NULL) {
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("type %s is not composite",
                            format_type_be(type_id))));
        }

        // Return stable identifier that changes with type definition
        return typentry->tupDesc_identifier;
    } else {
        // RECORD type - check if it's registered
        if (typmod >= 0 && typmod < RecordCacheArrayLen &&
            RecordCacheArray[typmod].tupdesc != NULL) {
            // Registered RECORD type - return stable identifier
            return RecordCacheArray[typmod].id;
        }

        // Anonymous RECORD type - generate new identifier each time
        return ++tupledesc_id_counter;
    }
}
```