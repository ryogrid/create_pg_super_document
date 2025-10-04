# make_tuple_indirect

## Location
[src/test/regress/regress.c:552-650](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/regress.c#L552-L650)

## Overview
A PostgreSQL test function that converts regular tuple attributes into indirect TOAST pointers for testing TOAST (The Oversized-Attribute Storage Technique) functionality with indirect references.

## Definition

```c
structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
```
## Detailed Description
The  function is a specialized test utility that transforms a regular PostgreSQL tuple by converting its variable-length attributes into indirect TOAST pointers. This function is designed to test PostgreSQL's TOAST mechanism, specifically the indirect pointer functionality. It takes a HeapTupleHeader as input, decomposes the tuple into its constituent values, and then for each variable-length attribute that meets certain criteria (not dropped, not null, variable length, not plain storage), it creates an indirect pointer that references the original data. The function creates a new tuple structure where the original data is stored separately and accessed through indirect pointers. This enables testing of TOAST detoasting behavior and indirect pointer handling throughout the PostgreSQL system.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro providing access to function call context and arguments
- : Input HeapTupleHeader containing the tuple data to be processed
- : Temporary HeapTupleData structure for tuple manipulation
- : Array of Datum values extracted from the original tuple
- : Array of boolean flags indicating null values
- : OID of the tuple's row type
- : Type modifier for the tuple type
- : Tuple descriptor containing metadata about the tuple structure

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract HeapTupleHeader from function arguments
  - : Extracts type OID from tuple header
  - : Extracts type modifier from tuple header
  - : Looks up tuple descriptor for row type
  - : Gets length of tuple data
  - : Sets item pointer to invalid state
  - : Decomposes tuple into values and nulls arrays
  - : Detoasts externally stored attributes
  - : Creates new tuple from values and nulls
  - : Releases tuple descriptor reference
  - /: PostgreSQL memory allocation functions
  - : PostgreSQL memory deallocation function
- Called from (representative examples):
  - : Referenced in the same test regression file

## Notes and Other Information
- This is a test function located in the PostgreSQL regression test suite
- The function operates specifically on variable-length attributes (attlen == -1)
- Skips attributes that are dropped, null, fixed-length, or have plain storage
- Does not recursively create indirect pointers for already-indirect attributes
- Uses TopTransactionContext for memory allocation to ensure data persistence
- The function intentionally violates the general rule about composite Datums containing TOAST pointers for testing purposes
- Critical for testing TOAST functionality, particularly indirect pointer detoasting
- The returned tuple contains indirect pointers that must be handled carefully to avoid premature flattening

## Simplified Source

```c
Datum make_tuple_indirect(PG_FUNCTION_ARGS) {
    // Extract tuple header and set up temporary tuple structure
    HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
    HeapTupleData tuple;
    tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
    tuple.t_data = rec;
    ItemPointerSetInvalid(&(tuple.t_self));
    tuple.t_tableOid = InvalidOid;

    // Get tuple type information and descriptor
    Oid tupType = HeapTupleHeaderGetTypeId(rec);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
    TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    int ncolumns = tupdesc->natts;

    // Extract tuple values and null flags
    Datum *values = (Datum *) palloc(ncolumns * sizeof(Datum));
    bool *nulls = (bool *) palloc(ncolumns * sizeof(bool));
    heap_deform_tuple(&tuple, tupdesc, values, nulls);

    // Switch to long-lived memory context for indirect pointers
    MemoryContext old_context = MemoryContextSwitchTo(TopTransactionContext);

    // Process each column to create indirect pointers
    for (int i = 0; i < ncolumns; i++) {
        // Skip inappropriate attributes: dropped, null, fixed-length, or plain storage
        if (TupleDescAttr(tupdesc, i)->attisdropped ||
            nulls[i] ||
            TupleDescAttr(tupdesc, i)->attlen != -1 ||
            TupleDescAttr(tupdesc, i)->attstorage == TYPSTORAGE_PLAIN)
            continue;

        struct varlena *attr = (struct varlena *) DatumGetPointer(values[i]);

        // Skip if already an indirect pointer
        if (VARATT_IS_EXTERNAL_INDIRECT(attr))
            continue;

        // Copy the attribute data to persistent storage
        if (VARATT_IS_EXTERNAL_ONDISK(attr)) {
            attr = detoast_external_attr(attr);
        } else {
            struct varlena *oldattr = attr;
            attr = palloc0(VARSIZE_ANY(oldattr));
            memcpy(attr, oldattr, VARSIZE_ANY(oldattr));
        }

        // Create indirect pointer structure
        struct varlena *new_attr = (struct varlena *) palloc0(INDIRECT_POINTER_SIZE);
        struct varatt_indirect redirect_pointer;
        redirect_pointer.pointer = attr;

        // Set up the indirect pointer
        SET_VARTAG_EXTERNAL(new_attr, VARTAG_INDIRECT);
        memcpy(VARDATA_EXTERNAL(new_attr), &redirect_pointer, sizeof(redirect_pointer));

        values[i] = PointerGetDatum(new_attr);
    }

    // Create new tuple with indirect pointers
    HeapTuple newtup = heap_form_tuple(tupdesc, values, nulls);

    // Cleanup
    pfree(values);
    pfree(nulls);
    ReleaseTupleDesc(tupdesc);
    MemoryContextSwitchTo(old_context);

    // Return tuple header with indirect pointers intact
    PG_RETURN_POINTER(newtup->t_data);
}
```