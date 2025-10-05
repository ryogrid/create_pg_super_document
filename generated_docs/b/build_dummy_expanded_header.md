# build_dummy_expanded_header

## Location
[src/backend/utils/adt/expandedrecord.c:1402-1493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L1402-L1493)

## Overview
Constructs a temporary "dummy" expanded record header used for domain constraint validation without modifying the state of the main expanded record.

## Definition

```c
struct dummy header to contain proposed new field set */
	build_dummy_expanded_header(erh);
```
## Detailed Description
This function creates a specialized dummy expanded record header that serves as a temporary workspace for domain constraint checking. The dummy header contains proposed field values that can be validated without affecting the main record's state. This approach ensures that constraint violations don't leave the main record in a corrupted state.

The function employs a lazy allocation strategy - creating the dummy header only when first needed, or when the field count changes. The dummy header shares metadata with the main record but maintains its own field value arrays. Importantly, it uses the short-term memory context to ensure any detoasted values created during constraint checking are automatically cleaned up.

## Parameters / Member Variables
- : Pointer to the main ExpandedRecordHeader that needs domain constraint validation

## Dependencies
- Functions called/Symbols referenced:
  - [expanded_record_get_tupdesc](../e/expanded_record_get_tupdesc.md)
  - [get_short_term_cxt](../g/get_short_term_cxt.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [EOH_init_header](../E/EOH_init_header.md)
  - ER_MAGIC
- Called from (representative examples):
  - [check_domain_for_new_field](../c/check_domain_for_new_field.md)
  - [check_domain_for_new_tuple](../c/check_domain_for_new_tuple.md)

## Notes and Other Information
- Function is marked static, indicating internal use within expandedrecord.c only
- Dummy header is marked with ER_FLAG_IS_DUMMY to distinguish it from regular headers
- Uses the short-term memory context to prevent memory leaks during constraint checking
- Copies composite type identification but not domain-specific flags from main header
- Reuses allocated dummy header across multiple constraint checks for efficiency
- Does not transfer domain flags since constraint checking operates on base type values
- System columns remain available through copied fvalue reference from main header

## Simplified Source

```c
static void build_dummy_expanded_header(ExpandedRecordHeader *main_erh)
{
    TupleDesc tupdesc = expanded_record_get_tupdesc(main_erh);
    get_short_term_cxt(main_erh); // Ensure short-term context exists

    ExpandedRecordHeader *erh = main_erh->er_dummy_header;

    // Allocate dummy header if needed or field count changed
    if (erh == NULL || erh->nfields != tupdesc->natts) {
        // Allocate header plus space for field arrays
        size_t header_size = MAXALIGN(sizeof(ExpandedRecordHeader));
        size_t data_size = tupdesc->natts * (sizeof(Datum) + sizeof(bool));

        erh = (ExpandedRecordHeader *) MemoryContextAlloc(main_erh->hdr.eoh_context,
                                                         header_size + data_size);
        memset(erh, 0, sizeof(ExpandedRecordHeader));

        // Initialize header
        EOH_init_header(&erh->hdr, &ER_methods, main_erh->er_short_term_cxt);
        erh->er_magic = ER_MAGIC;

        // Set up field arrays
        char *chunk = (char *) erh + header_size;
        erh->dvalues = (Datum *) chunk;
        erh->dnulls = (bool *) (chunk + tupdesc->natts * sizeof(Datum));
        erh->nfields = tupdesc->natts;

        main_erh->er_dummy_header = erh;
    }

    // Configure dummy header for constraint checking
    erh->flags = ER_FLAG_IS_DUMMY;
    erh->er_decltypeid = erh->er_typeid = main_erh->er_typeid;
    erh->er_typmod = main_erh->er_typmod;
    erh->er_tupdesc = tupdesc;
    erh->er_tupdesc_id = main_erh->er_tupdesc_id;
    erh->flat_size = 0;

    // Copy fvalue for system columns access
    erh->fvalue = main_erh->fvalue;
    erh->fstartptr = main_erh->fstartptr;
    erh->fendptr = main_erh->fendptr;
}
```