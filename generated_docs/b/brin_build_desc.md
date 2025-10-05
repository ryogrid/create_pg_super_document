# brin_build_desc

## Location
[src/backend/access/brin/brin.c:1572-1626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1572-L1626)

## Overview
Constructs and initializes a BrinDesc structure that contains all metadata needed to create or scan a BRIN index.

## Definition

```c
struct and fill it in */
	totalsize = offsetof(BrinDesc, bd_info) +
		sizeof(BrinOpcInfo *) * tupdesc->natts;
```
## Detailed Description
This function creates a comprehensive descriptor for a BRIN index by collecting opclass information for each indexed column. It allocates a dedicated memory context to manage the descriptor's lifetime, retrieves opclass-specific information using the BRIN_PROCNUM_OPCINFO procedure, and assembles all metadata into a BrinDesc structure. The function calculates the total number of stored columns across all opclasses, as this varies by opclass implementation. The resulting descriptor contains everything needed for tuple construction, scanning, and other BRIN operations.

## Parameters / Member Variables
- : The BRIN index relation for which to build the descriptor

## Dependencies
- Functions called/Symbols referenced:
  - : Creates dedicated memory context for the descriptor
  - : Switches to the new memory context
  - : Gets the tuple descriptor from the relation
  - : Allocates array for opclass info pointers
  - : Retrieves opclass procedure information
  - : Calls the opclass info function
  - : Accesses tuple descriptor attributes
  - : Converts function result to pointer
  - : Allocates the final BrinDesc structure
  - : Frees temporary allocations
  - : Memory context configuration constant
  - : Procedure number for opclass info
  - : The main descriptor structure type
  - : Per-column opclass information structure
- Called from (representative examples):
  - : During index tuple insertion setup
  - : When starting an index scan
  - : During index build operations

## Notes and Other Information
- Creates a dedicated memory context () for the descriptor's lifetime
- The descriptor persists beyond the function call - callers are responsible for cleanup
- Lazily initializes  (set to NULL initially, generated when first needed)
- The  field accumulates storage requirements across all opclasses
- Each opclass can define different numbers of stored values via 
- The  array contains one BrinOpcInfo pointer per indexed column
- Memory management follows PostgreSQL conventions with proper context switching
- The structure size is calculated dynamically based on the number of indexed attributes

## Simplified Source

```c
BrinDesc *
brin_build_desc(Relation rel)
{
    BrinOpcInfo **opcinfo;
    BrinDesc *bdesc;
    TupleDesc tupdesc;
    int totalstored = 0;
    int keyno;
    long totalsize;
    MemoryContext cxt, oldcxt;

    // Create dedicated memory context for descriptor
    cxt = AllocSetContextCreate(CurrentMemoryContext,
                                "brin desc cxt",
                                ALLOCSET_SMALL_SIZES);
    oldcxt = MemoryContextSwitchTo(cxt);
    tupdesc = RelationGetDescr(rel);

    // Get opclass information for each indexed column
    opcinfo = palloc_array(BrinOpcInfo *, tupdesc->natts);
    for (keyno = 0; keyno < tupdesc->natts; keyno++) {
        FmgrInfo *opcInfoFn;
        Form_pg_attribute attr = TupleDescAttr(tupdesc, keyno);

        opcInfoFn = index_getprocinfo(rel, keyno + 1, BRIN_PROCNUM_OPCINFO);
        opcinfo[keyno] = (BrinOpcInfo *)
            DatumGetPointer(FunctionCall1(opcInfoFn, attr->atttypid));
        totalstored += opcinfo[keyno]->oi_nstored;
    }

    // Allocate and initialize BrinDesc structure
    totalsize = offsetof(BrinDesc, bd_info) +
        sizeof(BrinOpcInfo *) * tupdesc->natts;

    bdesc = palloc(totalsize);
    bdesc->bd_context = cxt;
    bdesc->bd_index = rel;
    bdesc->bd_tupdesc = tupdesc;
    bdesc->bd_disktdesc = NULL;  // Generated lazily
    bdesc->bd_totalstored = totalstored;

    // Copy opclass info pointers
    for (keyno = 0; keyno < tupdesc->natts; keyno++)
        bdesc->bd_info[keyno] = opcinfo[keyno];
    pfree(opcinfo);

    MemoryContextSwitchTo(oldcxt);
    return bdesc;
}
```