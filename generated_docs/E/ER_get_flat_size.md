# ER_get_flat_size

## Location
[src/backend/utils/adt/expandedrecord.c:652-763](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L652-L763)

## Overview
ER_get_flat_size calculates the size required to store an expanded record in its flattened (serialized) composite datum format.

## Definition
static Size ER_get_flat_size(ExpandedObjectHeader *eohptr)

## Detailed Description
This function determines the total size needed to flatten an expanded record into a valid composite datum format. It handles several optimization scenarios:

1. **Early return for valid flattened values**: If the record already has a valid flattened representation without external references, it returns the cached size.
2. **Cached size optimization**: Returns previously calculated size if available.
3. **External field handling**: Detoasts any external (out-of-line) field values to ensure the flattened representation contains only inline data.
4. **Type registration**: Ensures anonymous RECORD types are properly registered with a valid typmod.
5. **Size calculation**: Computes the total space required including header, null bitmap, and data sections.

The function operates in a short-lived memory context to avoid memory leaks during detoasting operations.

## Parameters / Member Variables
- `eohptr`: Pointer to the ExpandedObjectHeader (cast to ExpandedRecordHeader internally) containing the expanded record to be sized

## Dependencies
- Functions called/Symbols referenced:
  - [expanded_record_get_tupdesc](../e/expanded_record_get_tupdesc.md)
  - [assign_record_type_typmod](../a/assign_record_type_typmod.md)
  - [deconstruct_expanded_record](../d/deconstruct_expanded_record.md)
  - VARATT_IS_EXTERNAL
  - [expanded_record_set_field_internal](../e/expanded_record_set_field_internal.md)
  - [heap_compute_data_size](../h/heap_compute_data_size.md)
  - BITMAPLEN
- Called from (representative examples):
  - No direct references found (likely called via function pointer in ExpandedObjectMethods)

## Notes and Other Information
- This is a method implementation for the expanded object infrastructure
- Caches calculated size, data length, header offset, and null flag information for future use
- Ensures composite datums contain no out-of-line values by detoasting external references
- Uses MAXALIGN to ensure proper data alignment in the flattened representation
- Part of PostgreSQL's expanded object system for efficient handling of complex data types

## Simplified Source

```c
static Size
ER_get_flat_size(ExpandedObjectHeader *eohptr)
{
    ExpandedRecordHeader *erh = (ExpandedRecordHeader *) eohptr;
    TupleDesc tupdesc;
    Size len, data_len;
    int hoff;
    bool hasnull;

    Assert(erh->er_magic == ER_MAGIC);

    // Ensure RECORD types have proper typmod
    if (erh->er_typeid == RECORDOID && erh->er_typmod < 0) {
        tupdesc = expanded_record_get_tupdesc(erh);
        assign_record_type_typmod(tupdesc);
        erh->er_typmod = tupdesc->tdtypmod;
    }

    // Fast path: valid flattened value without external fields
    if (erh->flags & ER_FLAG_FVALUE_VALID && !(erh->flags & ER_FLAG_HAVE_EXTERNAL))
        return erh->fvalue->t_len;

    // Return cached size if available
    if (erh->flat_size)
        return erh->flat_size;

    // Ensure deconstructed representation exists
    if (!(erh->flags & ER_FLAG_DVALUES_VALID))
        deconstruct_expanded_record(erh);

    tupdesc = erh->er_tupdesc;

    // Detoast any external field values
    if (erh->flags & ER_FLAG_HAVE_EXTERNAL) {
        for (int i = 0; i < erh->nfields; i++) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (!erh->dnulls[i] && !attr->attbyval && attr->attlen == -1 &&
                VARATT_IS_EXTERNAL(DatumGetPointer(erh->dvalues[i]))) {
                expanded_record_set_field_internal(erh, i + 1, erh->dvalues[i],
                                                  false, true, false);
            }
        }
        erh->flags &= ~ER_FLAG_HAVE_EXTERNAL;
    }

    // Check for null values
    hasnull = false;
    for (int i = 0; i < erh->nfields; i++) {
        if (erh->dnulls[i]) {
            hasnull = true;
            break;
        }
    }

    // Calculate total space needed
    len = offsetof(HeapTupleHeaderData, t_bits);
    if (hasnull)
        len += BITMAPLEN(tupdesc->natts);
    hoff = len = MAXALIGN(len);
    data_len = heap_compute_data_size(tupdesc, erh->dvalues, erh->dnulls);
    len += data_len;

    // Cache results
    erh->flat_size = len;
    erh->data_len = data_len;
    erh->hoff = hoff;
    erh->hasnull = hasnull;

    return len;
}
```