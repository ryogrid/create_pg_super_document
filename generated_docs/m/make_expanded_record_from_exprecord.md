# make_expanded_record_from_exprecord

## Location
[src/backend/utils/adt/expandedrecord.c:329-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L329-L439)

## Overview
Creates a new expanded record with the same rowtype as an existing expanded record, optimized by bypassing type cache lookups and copying only type metadata.

## Definition
```c
ExpandedRecordHeader *make_expanded_record_from_exprecord(ExpandedRecordHeader *olderh, MemoryContext parentcontext)
```

## Detailed Description
This function builds an expanded record of the same rowtype as the given expanded record, providing a performance optimization over the other creation methods by avoiding type cache lookups. It copies type identification information and tuple descriptor management strategy from the source expanded record, but creates a completely new, empty record instance.

The function intelligently handles tuple descriptor sharing based on the source record's management approach: using reference counting for refcounted descriptors, copying when the source has a private copy, or assuming persistence when the source uses a shared descriptor. The resulting record inherits only the IS_DOMAIN flag from the source, with all other state flags reset to maintain the empty initialization.

## Parameters / Member Variables
- `olderh`: The existing ExpandedRecordHeader to copy the rowtype structure from
- `parentcontext`: Memory context that will be the parent of the new expanded object's private context

## Dependencies
- Functions called/Symbols referenced:
  - [expanded_record_get_tupdesc](../e/expanded_record_get_tupdesc.md)
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [EOH_init_header](../E/EOH_init_header.md)
  - [MemoryContextRegisterResetCallback](../M/MemoryContextRegisterResetCallback.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from:
  - (No direct references found in the analyzed codebase)

## Notes and Other Information
- Provides significant performance benefits by avoiding type cache lookups that the other creation functions require
- Does not copy any tuple data from the source expanded record, only the structural type information
- Inherits only the ER_FLAG_IS_DOMAIN flag from the source record while resetting all other state flags
- Uses the same tuple descriptor management strategy as the source record (refcounting, copying, or sharing)
- The new record is initialized in an empty state without setting ER_FLAG_DVALUES_VALID or ER_FLAG_FVALUE_VALID
- Optimizes memory allocation by pre-allocating dvalues/dnulls arrays alongside the header structure

## Simplified Source

```c
ExpandedRecordHeader *
make_expanded_record_from_exprecord(ExpandedRecordHeader *olderh, MemoryContext parentcontext)
{
    ExpandedRecordHeader *erh;
    TupleDesc tupdesc = expanded_record_get_tupdesc(olderh);
    MemoryContext objcxt;

    // Create memory context for new expanded object
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

    // Copy type identification from source record
    erh->er_decltypeid = olderh->er_decltypeid;
    erh->er_typeid = olderh->er_typeid;
    erh->er_typmod = olderh->er_typmod;
    erh->er_tupdesc_id = olderh->er_tupdesc_id;

    // Inherit only IS_DOMAIN flag from source
    erh->flags = olderh->flags & ER_FLAG_IS_DOMAIN;

    // Handle tupdesc sharing based on source's strategy
    if (tupdesc->tdrefcount >= 0) {
        // Use reference counting with cleanup callback
        erh->er_mcb.func = ER_mc_callback;
        erh->er_mcb.arg = (void *) erh;
        MemoryContextRegisterResetCallback(erh->hdr.eoh_context, &erh->er_mcb);
        erh->er_tupdesc = tupdesc;
        tupdesc->tdrefcount++;
    } else if (olderh->flags & ER_FLAG_TUPDESC_ALLOCED) {
        // Source has private copy, so we need our own copy
        MemoryContext oldcxt = MemoryContextSwitchTo(objcxt);
        erh->er_tupdesc = CreateTupleDescCopy(tupdesc);
        erh->flags |= ER_FLAG_TUPDESC_ALLOCED;
        MemoryContextSwitchTo(oldcxt);
    } else {
        // Assume tupdesc persists like in source record
        erh->er_tupdesc = tupdesc;
    }

    return erh;
}
```