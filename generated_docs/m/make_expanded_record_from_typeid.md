# make_expanded_record_from_typeid

## Location
[src/backend/utils/adt/expandedrecord.c:69-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L69-L204)

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
  - [EOH_init_header](../E/EOH_init_header.md)
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

## Simplified Source

```c
ExpandedRecordHeader *make_expanded_record_from_typeid(Oid type_id, int32 typmod, MemoryContext parentcontext) {
    int flags = 0;
    TupleDesc tupdesc;
    uint64 tupdesc_id;

    // Handle different type cases
    if (type_id != RECORDOID) {
        // Look up composite type information
        TypeCacheEntry *typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO);

        if (typentry->typtype == TYPTYPE_DOMAIN) {
            flags |= ER_FLAG_IS_DOMAIN;
            typentry = lookup_type_cache(typentry->domainBaseType, TYPECACHE_TUPDESC);
        }

        if (typentry->tupDesc == NULL)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("type %s is not composite", format_type_be(type_id))));

        tupdesc = typentry->tupDesc;
        tupdesc_id = typentry->tupDesc_identifier;
    } else {
        // Handle RECORD types
        tupdesc = lookup_rowtype_tupdesc(type_id, typmod);
        tupdesc_id = assign_record_type_identifier(type_id, typmod);
    }

    // Create memory context for expanded object
    MemoryContext objcxt = AllocSetContextCreate(parentcontext, "expanded record", ALLOCSET_DEFAULT_SIZES);

    // Allocate expanded record header with space for dvalues/dnulls arrays
    ExpandedRecordHeader *erh = (ExpandedRecordHeader *)
        MemoryContextAlloc(objcxt, MAXALIGN(sizeof(ExpandedRecordHeader)) +
                          tupdesc->natts * (sizeof(Datum) + sizeof(bool)));

    // Initialize header
    memset(erh, 0, sizeof(ExpandedRecordHeader));
    EOH_init_header(&erh->hdr, &ER_methods, objcxt);
    erh->er_magic = ER_MAGIC;

    // Set up dvalues/dnulls arrays
    char *chunk = (char *) erh + MAXALIGN(sizeof(ExpandedRecordHeader));
    erh->dvalues = (Datum *) chunk;
    erh->dnulls = (bool *) (chunk + tupdesc->natts * sizeof(Datum));
    erh->nfields = tupdesc->natts;

    // Fill in type identification
    erh->er_decltypeid = type_id;
    erh->er_typeid = tupdesc->tdtypeid;
    erh->er_typmod = tupdesc->tdtypmod;
    erh->er_tupdesc_id = tupdesc_id;
    erh->flags = flags;

    // Handle tuple descriptor reference counting
    if (tupdesc->tdrefcount >= 0) {
        erh->er_mcb.func = ER_mc_callback;
        erh->er_mcb.arg = (void *) erh;
        MemoryContextRegisterResetCallback(erh->hdr.eoh_context, &erh->er_mcb);
        erh->er_tupdesc = tupdesc;
        tupdesc->tdrefcount++;

        if (type_id == RECORDOID)
            ReleaseTupleDesc(tupdesc);
    } else {
        erh->er_tupdesc = tupdesc;
    }

    return erh;
}
```