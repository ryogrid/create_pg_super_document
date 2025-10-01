# expanded_record_fetch_tupdesc

## Location
[src/backend/utils/adt/expandedrecord.c:824-883](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L824-L883)

## Overview
expanded_record_fetch_tupdesc retrieves and caches the tuple descriptor for an expanded record's data type, handling reference counting and memory management.

## Definition
TupleDesc expanded_record_fetch_tupdesc(ExpandedRecordHeader *erh)

## Detailed Description
This function is the out-of-line portion of the expanded_record_get_tupdesc inline function. It performs the expensive operation of looking up a tuple descriptor when it is not already cached in the expanded record header.

Key responsibilities:
1. **Type lookup**: Uses the type cache to retrieve the tuple descriptor for the records type ID and type modifier
2. **Reference count management**: For refcounted tuple descriptors, it registers a memory context callback to properly manage the reference count lifecycle
3. **Caching**: Stores the retrieved tuple descriptor in the expanded record header for future access
4. **Global ID assignment**: Assigns a process-global identifier for the tuple descriptor

The function distinguishes between statically allocated tuple descriptors (tdrefcount < 0) and refcounted ones, applying appropriate memory management strategies for each.

## Parameters / Member Variables
- `erh`: Pointer to the ExpandedRecordHeader whose tuple descriptor needs to be fetched and cached

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - [ER_mc_callback](../E/ER_mc_callback.md)
  - [MemoryContextRegisterResetCallback](../M/MemoryContextRegisterResetCallback.md)
  - ReleaseTupleDesc
  - [assign_record_type_identifier](../a/assign_record_type_identifier.md)
- Called from (representative examples):
  - [expanded_record_get_tupdesc](expanded_record_get_tupdesc.md) (inline function in expandedrecord.h)

## Notes and Other Information
- This is the slow path for tuple descriptor retrieval - the fast path checks erh->er_tupdesc directly
- Uses memory context callbacks to manage tuple descriptor reference counts independent of ResourceOwner lifetime
- Handles both refcounted and non-refcounted tuple descriptors appropriately
- The function ensures proper cleanup by registering ER_mc_callback when managing refcounted descriptors
- Part of PostgreSQL's expanded object infrastructure for efficient composite type handling
- Internal code can access erh->er_tupdesc directly when ER_FLAG_DVALUES_VALID is set

## Simplified Source

```c
TupleDesc
expanded_record_fetch_tupdesc(ExpandedRecordHeader *erh)
{
    TupleDesc tupdesc;

    // Return cached descriptor if available
    if (erh->er_tupdesc)
        return erh->er_tupdesc;

    // Look up the composite type's tupdesc
    tupdesc = lookup_rowtype_tupdesc(erh->er_typeid, erh->er_typmod);

    if (tupdesc->tdrefcount >= 0)
    {
        // Refcounted tupdesc: manage with memory context callback
        if (erh->er_mcb.arg == NULL)
        {
            erh->er_mcb.func = ER_mc_callback;
            erh->er_mcb.arg = (void *) erh;
            MemoryContextRegisterResetCallback(erh->hdr.eoh_context,
                                               &erh->er_mcb);
        }

        // Cache and increment reference count
        erh->er_tupdesc = tupdesc;
        tupdesc->tdrefcount++;

        // Release the pin from lookup_rowtype_tupdesc
        ReleaseTupleDesc(tupdesc);
    }
    else
    {
        // Static tupdesc: just cache the pointer
        erh->er_tupdesc = tupdesc;
    }

    // Get process-global ID for this tupdesc
    erh->er_tupdesc_id = assign_record_type_identifier(tupdesc->tdtypeid,
                                                       tupdesc->tdtypmod);

    return tupdesc;
}
```