# make_expanded_record_from_tupdesc

## Location
[src/backend/utils/adt/expandedrecord.c:205-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L205-L328)

## Overview
Creates an expanded record object from a given TupleDesc, copying the tupdesc if necessary or incrementing its reference count when possible.

## Definition
```c
ExpandedRecordHeader *make_expanded_record_from_tupdesc(TupleDesc tupdesc, MemoryContext parentcontext)
```

## Detailed Description
This function builds an expanded record based on the rowtype defined by the provided TupleDesc. It intelligently handles tuple descriptor management by preferring to reference the type cache's copy for named composite types (which guarantees reference counting) while copying the tupdesc when necessary for other cases.

For named composite types (non-RECORD types), the function consults the type cache to obtain the canonical refcounted version of the tuple descriptor and the correct tupdesc identifier. For RECORD types, it assigns a unique identifier while using the provided tupdesc. The resulting expanded record is initialized in an "empty" state logically equivalent to a NULL composite value.

## Parameters / Member Variables
- `tupdesc`: The TupleDesc defining the structure of the record to be created
- `parentcontext`: Memory context that will be the parent of the expanded object's private context

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [assign_record_type_identifier](../a/assign_record_type_identifier.md)
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [EOH_init_header](../E/EOH_init_header.md)
  - [MemoryContextRegisterResetCallback](../M/MemoryContextRegisterResetCallback.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from:
  - (No direct references found in the analyzed codebase)

## Notes and Other Information
- Prefers to use type cache copies of tuple descriptors for named composite types to ensure proper reference counting
- Automatically handles both refcounted and non-refcounted tuple descriptors appropriately
- For refcounted tupdescs, uses memory context callbacks to manage reference counting lifecycle
- For non-refcounted tupdescs, creates a private copy using CreateTupleDescCopy and sets ER_FLAG_TUPDESC_ALLOCED
- Uses regular-size memory context to improve odds of fitting tuple descriptors without extra allocations
- The resulting record does not have field validity flags set, maintaining the "empty" state until explicitly populated

## Simplified Source

```c
ExpandedRecordHeader *
make_expanded_record_from_tupdesc(TupleDesc tupdesc, MemoryContext parentcontext)
{
    ExpandedRecordHeader *erh;
    uint64 tupdesc_id;
    MemoryContext objcxt;

    // Handle named composite types vs RECORD types
    if (tupdesc->tdtypeid != RECORDOID) {
        // Use type cache for named composite types
        TypeCacheEntry *typentry = lookup_type_cache(tupdesc->tdtypeid, TYPECACHE_TUPDESC);
        if (typentry->tupDesc == NULL)
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                           errmsg("type %s is not composite", format_type_be(tupdesc->tdtypeid))));
        tupdesc = typentry->tupDesc;
        tupdesc_id = typentry->tupDesc_identifier;
    } else {
        // For RECORD types, assign unique identifier
        tupdesc_id = assign_record_type_identifier(tupdesc->tdtypeid, tupdesc->tdtypmod);
    }

    // Create memory context for expanded object
    objcxt = AllocSetContextCreate(parentcontext, "expanded record", ALLOCSET_DEFAULT_SIZES);

    // Allocate header with space for dvalues/dnulls arrays
    erh = (ExpandedRecordHeader *)
        MemoryContextAlloc(objcxt, MAXALIGN(sizeof(ExpandedRecordHeader))
                          + tupdesc->natts * (sizeof(Datum) + sizeof(bool)));

    // Initialize header and set up arrays
    memset(erh, 0, sizeof(ExpandedRecordHeader));
    EOH_init_header(&erh->hdr, &ER_methods, objcxt);
    erh->er_magic = ER_MAGIC;

    char *chunk = (char *) erh + MAXALIGN(sizeof(ExpandedRecordHeader));
    erh->dvalues = (Datum *) chunk;
    erh->dnulls = (bool *) (chunk + tupdesc->natts * sizeof(Datum));
    erh->nfields = tupdesc->natts;

    // Set type identification info
    erh->er_decltypeid = erh->er_typeid = tupdesc->tdtypeid;
    erh->er_typmod = tupdesc->tdtypmod;
    erh->er_tupdesc_id = tupdesc_id;

    // Handle tupdesc copying/refcounting
    if (tupdesc->tdrefcount >= 0) {
        // Use reference counting with cleanup callback
        erh->er_mcb.func = ER_mc_callback;
        erh->er_mcb.arg = (void *) erh;
        MemoryContextRegisterResetCallback(erh->hdr.eoh_context, &erh->er_mcb);
        erh->er_tupdesc = tupdesc;
        tupdesc->tdrefcount++;
    } else {
        // Copy the tupdesc
        MemoryContext oldcxt = MemoryContextSwitchTo(objcxt);
        erh->er_tupdesc = CreateTupleDescCopy(tupdesc);
        erh->flags |= ER_FLAG_TUPDESC_ALLOCED;
        MemoryContextSwitchTo(oldcxt);
    }

    return erh;
}
```