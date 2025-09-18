# make_expanded_record_from_typeid

## Location
src/backend/utils/adt/expandedrecord.c: 69 - 204

## Overview
Creates an expanded record object from a given composite type OID and typmod, initializing it in an "empty" state logically equivalent to a NULL composite value.

## Definition
```c
ExpandedRecordHeader *make_expanded_record_from_typeid(Oid type_id, int32 typmod, MemoryContext parentcontext)
```

## Detailed Description
This function builds an expanded record of the specified composite type. The function handles both regular composite types and RECORD types (when type_id is RECORDOID with a positive typmod). It creates a memory context for the expanded object and initializes all necessary data structures including the dvalues/dnulls arrays for field storage.

The function performs type validation through the type cache, ensuring the specified type is actually composite. For domain types over composite types, it automatically resolves to the base composite type while setting appropriate flags. The resulting expanded record is initially in an "empty" state, which may not be valid for domain types that require validation.

## Parameters / Member Variables
- `type_id`: The OID of the composite type to create an expanded record for (can be RECORDOID if typmod > 0)
- `typmod`: Type modifier for the composite type (required to be positive for RECORDOID)
- `parentcontext`: Memory context that will be the parent of the expanded object's private context

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - [assign_record_type_identifier](../a/assign_record_type_identifier.md)
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - EOH_init_header
  - [MemoryContextRegisterResetCallback](../M/MemoryContextRegisterResetCallback.md)
  - ReleaseTupleDesc
- Called from:
  - (No direct references found in the analyzed codebase)

## Notes and Other Information
- The function allocates dvalues/dnulls arrays preemptively even though they may not be immediately needed
- For refcounted tuple descriptors, the function manages reference counting via memory context callbacks
- The resulting expanded record does not have ER_FLAG_DVALUES_VALID or ER_FLAG_FVALUE_VALID set, maintaining the "empty" state
- Domain type validity checking is deferred to callers who should use expanded_record_set_tuple(erh, NULL, false, false) if needed
- Uses regular-size memory context allocation to improve chances of fitting tuple descriptors without extra malloc blocks