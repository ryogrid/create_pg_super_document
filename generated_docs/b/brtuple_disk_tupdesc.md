# brtuple_disk_tupdesc

## Location
[src/backend/access/brin/brin_tuple.c:61-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_tuple.c#L61-L98)

## Overview
Returns a tuple descriptor used for on-disk storage of BRIN tuples, providing the structure needed to serialize BRIN index data to disk.

## Definition

```c
static TupleDesc
brtuple_disk_tupdesc(BrinDesc *brdesc)
```
## Detailed Description
This function creates and caches a tuple descriptor that defines the on-disk storage format for BRIN (Block Range Index) tuples. The function builds a template tuple descriptor based on the BRIN descriptor's stored attribute information, ensuring that each stored value from the BRIN opclass gets a corresponding entry in the disk tuple descriptor. The tuple descriptor is cached in the BrinDesc structure to avoid repeated creation, and memory allocation is performed in the BrinDesc's memory context to ensure proper lifecycle management.

The function iterates through each attribute in the BRIN descriptor and for each attribute, processes all stored values defined by the opclass, creating tuple descriptor entries with the appropriate type information from the type cache.

## Parameters / Member Variables
- `*brdesc`: Pointer to BrinDesc structure containing BRIN descriptor information including attribute details, opclass info, and memory context for caching
## Dependencies
- Functions called/Symbols referenced:
  - [BrinDesc](../B/BrinDesc.md) (structure type)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md)
  - [BrinTuple](../B/BrinTuple.md) (structure type)
- Called from:
  - [brin_form_tuple](brin_form_tuple.md) (src/backend/access/brin/brin_tuple.c:283, 302)
  - [brin_deconstruct_tuple](brin_deconstruct_tuple.md) (src/backend/access/brin/brin_tuple.c:685)

## Notes and Other Information
- This is a static function, only accessible within brin_tuple.c
- The function implements caching by storing the result in brdesc->bd_disktdesc to avoid repeated computation
- Memory allocation is carefully managed using the BrinDesc's memory context
- The function handles variable numbers of stored values per attribute as defined by the BRIN opclass
- Essential for BRIN tuple serialization and deserialization operations

## Simplified Source

```c
static TupleDesc brtuple_disk_tupdesc(BrinDesc *brdesc)
{
    // Use cached version if available
    if (brdesc->bd_disktdesc == NULL) {
        // Switch to BrinDesc's memory context for persistence
        MemoryContext oldcxt = MemoryContextSwitchTo(brdesc->bd_context);

        // Create template with total number of stored attributes
        TupleDesc tupdesc = CreateTemplateTupleDesc(brdesc->bd_totalstored);

        // Build tuple descriptor entries for each stored value
        AttrNumber attno = 1;
        for (int i = 0; i < brdesc->bd_tupdesc->natts; i++) {
            for (int j = 0; j < brdesc->bd_info[i]->oi_nstored; j++) {
                TupleDescInitEntry(tupdesc, attno++, NULL,
                                 brdesc->bd_info[i]->oi_typcache[j]->type_id,
                                 -1, 0);
            }
        }

        MemoryContextSwitchTo(oldcxt);
        brdesc->bd_disktdesc = tupdesc;
    }

    return brdesc->bd_disktdesc;
}
```