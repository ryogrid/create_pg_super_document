# update_cached_tupdesc

## Location
[src/backend/utils/adt/jsonfuncs.c:3027-3055](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L3027-L3055)

## Overview
Acquires or updates the cached tuple descriptor for a composite type, ensuring the cache contains the correct type information for data population operations.

## Definition
```c
static void update_cached_tupdesc(CompositeIOData *io, MemoryContext mcxt)
```

## Detailed Description
update_cached_tupdesc manages the tuple descriptor cache within CompositeIOData structures. The function performs lazy initialization and validation of cached tuple descriptors, ensuring they remain synchronized with the current type definition. When the cache is empty, outdated, or doesn't match the expected type ID and type modifier, the function:

1. **Looks up current type information** using lookup_rowtype_tupdesc()
2. **Cleans up the old cache** by freeing the existing descriptor if present
3. **Creates a new cache entry** by copying the fresh descriptor into the specified memory context
4. **Releases temporary references** to prevent memory leaks

The function ensures type safety by validating that cached descriptors match both the type ID and type modifier of the expected composite type.

## Parameters / Member Variables
- `io`: CompositeIOData structure containing the tuple descriptor cache and type identification
- `mcxt`: Target memory context where the cached tuple descriptor should be allocated

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - ReleaseTupleDesc
- Called from (representative examples):
  - [populate_composite](../p/populate_composite.md)
  - [populate_recordset_record](../p/populate_recordset_record.md)
  - [populate_recordset_worker](../p/populate_recordset_worker.md)

## Notes and Other Information
- Uses a copy-without-constraints strategy via CreateTupleDescCopy() to avoid unnecessary overhead in the cache
- Properly manages memory contexts to ensure cached descriptors persist in the correct memory context
- Validates both type ID (tdtypeid) and type modifier (tdtypmod) to detect type changes that require cache updates
- Follows PostgreSQL's reference counting pattern with ReleaseTupleDesc() to properly manage shared tuple descriptor resources
- The cache invalidation check handles cases where the composite type definition has changed during the session

## Simplified Source

```c
static void update_cached_tupdesc(CompositeIOData *io, MemoryContext mcxt)
{
    // Check if cache needs update (missing, wrong type ID, or wrong type modifier)
    if (!io->tupdesc ||
        io->tupdesc->tdtypeid != io->base_typid ||
        io->tupdesc->tdtypmod != io->base_typmod)
    {
        // Lookup current tuple descriptor for the composite type
        TupleDesc tupdesc = lookup_rowtype_tupdesc(io->base_typid, io->base_typmod);
        MemoryContext oldcxt;

        // Free old cached descriptor if it exists
        if (io->tupdesc)
            FreeTupleDesc(io->tupdesc);

        // Copy new descriptor into cache memory context
        oldcxt = MemoryContextSwitchTo(mcxt);
        io->tupdesc = CreateTupleDescCopy(tupdesc);
        MemoryContextSwitchTo(oldcxt);

        // Release temporary reference to lookup result
        ReleaseTupleDesc(tupdesc);
    }
}
```