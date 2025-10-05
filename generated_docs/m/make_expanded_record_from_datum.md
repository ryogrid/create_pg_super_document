# make_expanded_record_from_datum

## Location
[src/backend/utils/adt/expandedrecord.c:580-651](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L580-L651)

## Overview
Creates an expanded record directly from a composite Datum, combining record creation and tuple assignment while deferring tuple descriptor lookup for performance optimization.

## Definition
```c
Datum make_expanded_record_from_datum(Datum recorddatum, MemoryContext parentcontext)
```

## Detailed Description
This function builds an expanded record from a composite Datum, effectively combining the functionality of make_expanded_record_from_typeid and expanded_record_set_tuple in a single optimized operation. It extracts type information directly from the tuple header and defers tuple descriptor lookup until actually needed, providing performance benefits when the tupdesc might never be accessed.

The function detoasts and copies the source record into the expanded object's private memory context, handling HeapTuple creation and initialization. It sets up the flat representation immediately but leaves the deconstructed representation and tuple descriptor lookup for later, making it particularly efficient for cases where only the flat representation is needed.

## Parameters / Member Variables
- `recorddatum`: The composite Datum to create an expanded record from
- `parentcontext`: Memory context that will be the parent of the expanded object's private context

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [EOH_init_header](../E/EOH_init_header.md)
  - DatumGetHeapTupleHeader
  - HeapTupleHeaderGetDatumLength
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - HeapTupleHeaderGetTypeId
  - HeapTupleHeaderGetTypMod
  - HeapTupleHeaderHasExternal
  - [EOHPGetRWDatum](../E/EOHPGetRWDatum.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from:
  - [DatumGetExpandedRecord](../D/DatumGetExpandedRecord.md)

## Notes and Other Information
- Optimizes performance by deferring tuple descriptor lookup until needed, unlike other creation functions
- Does not handle domain types since composite Datums cannot self-identify as domain types
- Automatically detoasts the input datum and creates a private copy in the expanded object's context
- Sets ER_FLAG_FVALUE_VALID and ER_FLAG_FVALUE_ALLOCED to indicate a valid flat representation
- Asserts that the input datum does not have external fields (should have been detoasted already)
- Returns a read/write Datum pointer to the expanded record using EOHPGetRWDatum
- Does not populate flat_size information or create deconstructed representation initially
- Uses MemoryContextAllocZero to ensure all header fields start as 0/null for proper initialization

## Simplified Source

```c
Datum
make_expanded_record_from_datum(Datum recorddatum, MemoryContext parentcontext)
{
    ExpandedRecordHeader *erh;
    HeapTupleHeader tuphdr;
    HeapTupleData tmptup;
    HeapTuple newtuple;
    MemoryContext objcxt;

    // Create memory context for expanded object
    objcxt = AllocSetContextCreate(parentcontext, "expanded record", ALLOCSET_DEFAULT_SIZES);

    // Initialize expanded record header
    erh = (ExpandedRecordHeader *) MemoryContextAllocZero(objcxt, sizeof(ExpandedRecordHeader));
    EOH_init_header(&erh->hdr, &ER_methods, objcxt);
    erh->er_magic = ER_MAGIC;

    // Extract tuple header from datum and set up temporary tuple
    tuphdr = DatumGetHeapTupleHeader(recorddatum);
    tmptup.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
    ItemPointerSetInvalid(&(tmptup.t_self));
    tmptup.t_tableOid = InvalidOid;
    tmptup.t_data = tuphdr;

    // Copy tuple into private context
    MemoryContext oldcxt = MemoryContextSwitchTo(objcxt);
    newtuple = heap_copytuple(&tmptup);
    erh->flags |= ER_FLAG_FVALUE_ALLOCED;
    MemoryContextSwitchTo(oldcxt);

    // Set type identification from tuple header
    erh->er_decltypeid = erh->er_typeid = HeapTupleHeaderGetTypeId(tuphdr);
    erh->er_typmod = HeapTupleHeaderGetTypMod(tuphdr);

    // Set up flat representation
    erh->fvalue = newtuple;
    erh->fstartptr = (char *) newtuple->t_data;
    erh->fendptr = ((char *) newtuple->t_data) + newtuple->t_len;
    erh->flags |= ER_FLAG_FVALUE_VALID;

    Assert(!HeapTupleHeaderHasExternal(tuphdr));

    return EOHPGetRWDatum(&erh->hdr);
}
```