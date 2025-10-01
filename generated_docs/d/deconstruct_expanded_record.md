# deconstruct_expanded_record

## Location
[src/backend/utils/adt/expandedrecord.c:952-1016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L952-L1016)

## Overview
Creates or validates the Datum/isnull array representation of an expanded record object, enabling direct access to individual field values without using accessor functions.

## Definition

```c
void
deconstruct_expanded_record(ExpandedRecordHeader *erh)
```
## Detailed Description
This function ensures that the dvalues and dnulls arrays in an expanded record are populated and valid. It serves as a lazy initialization mechanism for the flattened representation of the record's fields. The function first checks if the ER_FLAG_DVALUES_VALID flag is set; if so, it returns immediately as the arrays are already valid.

If the arrays need to be created or updated, the function allocates memory for both the Datum array (dvalues) and boolean array (dnulls) in a single palloc chunk within the expanded record's memory context. This optimization reduces memory allocation overhead.

The function handles two scenarios for populating the arrays:
1. If ER_FLAG_FVALUE_VALID is set (meaning a HeapTuple exists), it deconstructs the tuple using heap_deform_tuple
2. If no tuple exists (empty record), it initializes all fields as nulls

After completion, the function sets the ER_FLAG_DVALUES_VALID flag to indicate the arrays are ready for direct access.

## Parameters / Member Variables
- : Pointer to the ExpandedRecordHeader structure to be deconstructed

## Dependencies
- Functions called/Symbols referenced:
  - [expanded_record_get_tupdesc](../e/expanded_record_get_tupdesc.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - memset (for null initialization)
- Types referenced:
  - ExpandedRecordHeader
  - [TupleDesc](../T/TupleDesc.md)
  - Datum
- Flags used:
  - ER_FLAG_DVALUES_VALID
  - ER_FLAG_FVALUE_VALID
- Called from (representative examples):
  - [ER_get_flat_size](../E/ER_get_flat_size.md)
  - [expanded_record_fetch_field](../e/expanded_record_fetch_field.md)
  - [expanded_record_set_field_internal](../e/expanded_record_set_field_internal.md)
  - [expanded_record_set_fields](../e/expanded_record_set_fields.md)
  - [check_domain_for_new_field](../c/check_domain_for_new_field.md)

## Notes and Other Information
- The function uses a memory optimization by allocating both Datum and boolean arrays in a single chunk
- If the number of fields changes, old arrays may be leaked but this is considered acceptable
- Converting an empty record results in a row of all null values
- After calling this function, it's safe to read dvalues/dnulls arrays directly
- The function is idempotent - multiple calls are safe and efficient

## Simplified Source

```c
void deconstruct_expanded_record(ExpandedRecordHeader *erh) {
    TupleDesc tupdesc;
    Datum *dvalues;
    bool *dnulls;
    int nfields;

    // Return early if arrays are already valid
    if (erh->flags & ER_FLAG_DVALUES_VALID) {
        return;
    }

    // Get tuple descriptor for field information
    tupdesc = expanded_record_get_tupdesc(erh);
    nfields = tupdesc->natts;

    // Allocate or reuse Datum and null arrays
    if (erh->dvalues == NULL || erh->nfields != nfields) {
        // Allocate both arrays in single memory chunk for efficiency
        char *chunk = MemoryContextAlloc(erh->hdr.eoh_context,
                                        nfields * (sizeof(Datum) + sizeof(bool)));
        dvalues = (Datum *) chunk;
        dnulls = (bool *) (chunk + nfields * sizeof(Datum));

        erh->dvalues = dvalues;
        erh->dnulls = dnulls;
        erh->nfields = nfields;
    } else {
        dvalues = erh->dvalues;
        dnulls = erh->dnulls;
    }

    // Populate arrays based on record state
    if (erh->flags & ER_FLAG_FVALUE_VALID) {
        // Deconstruct existing tuple into arrays
        heap_deform_tuple(erh->fvalue, tupdesc, dvalues, dnulls);
    } else {
        // Initialize empty record as all nulls
        memset(dvalues, 0, nfields * sizeof(Datum));
        memset(dnulls, true, nfields * sizeof(bool));
    }

    // Mark arrays as valid for direct access
    erh->flags |= ER_FLAG_DVALUES_VALID;
}
```