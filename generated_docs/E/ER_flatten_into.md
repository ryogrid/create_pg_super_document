# ER_flatten_into

## Location
[src/backend/utils/adt/expandedrecord.c:764-823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L764-L823)

## Overview
ER_flatten_into serializes an expanded record into a flattened composite datum format at a specified memory location.

## Definition
static void ER_flatten_into(ExpandedObjectHeader *eohptr, void *result, Size allocated_size)

## Detailed Description
This function flattens an expanded record into a standard HeapTuple format that can be stored or transmitted. It provides two optimization paths:

1. **Fast path**: If the record has a valid cached flattened representation without external references, it performs a simple memcpy operation and updates the datum header fields.

2. **Full reconstruction**: When no cached representation is available, it constructs the flattened tuple from scratch using the expanded records dvalues and dnulls arrays.

The function ensures proper initialization of all header fields including datum length, type information, and tuple metadata. It also guarantees that pad space is zero-filled for consistent binary representation.

## Parameters / Member Variables
- `eohptr`: Pointer to the ExpandedObjectHeader containing the expanded record to flatten
- `result`: Destination buffer where the flattened tuple will be written
- `allocated_size`: Size of the destination buffer (must match the size calculated by ER_get_flat_size)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderSetDatumLength
  - HeapTupleHeaderSetTypeId
  - HeapTupleHeaderSetTypMod
  - [expanded_record_get_tupdesc](../e/expanded_record_get_tupdesc.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - HeapTupleHeaderSetNatts
  - [heap_fill_tuple](../h/heap_fill_tuple.md)
- Called from (representative examples):
  - No direct references found (likely called via function pointer in ExpandedObjectMethods)

## Notes and Other Information
- This is a method implementation for the expanded object infrastructure
- Uses memset to ensure all padding bytes are zero-filled for consistent binary representation
- Sets t_ctid to invalid value since flattened composite datums do not have valid tuple identifiers
- The fast path optimization avoids reconstruction when a valid cached representation exists
- Works in conjunction with ER_get_flat_size to ensure proper memory allocation
- Part of PostgreSQL's expanded object system for efficient serialization of complex data types

## Simplified Source

```c
static void
ER_flatten_into(ExpandedObjectHeader *eohptr, void *result, Size allocated_size)
{
    ExpandedRecordHeader *erh = (ExpandedRecordHeader *) eohptr;
    HeapTupleHeader tuphdr = (HeapTupleHeader) result;
    TupleDesc tupdesc;

    Assert(erh->er_magic == ER_MAGIC);

    // Fast path: copy existing valid flattened value
    if (erh->flags & ER_FLAG_FVALUE_VALID && !(erh->flags & ER_FLAG_HAVE_EXTERNAL)) {
        Assert(allocated_size == erh->fvalue->t_len);
        memcpy(tuphdr, erh->fvalue->t_data, allocated_size);
        // Update header fields
        HeapTupleHeaderSetDatumLength(tuphdr, allocated_size);
        HeapTupleHeaderSetTypeId(tuphdr, erh->er_typeid);
        HeapTupleHeaderSetTypMod(tuphdr, erh->er_typmod);
        return;
    }

    // Full reconstruction path
    Assert(allocated_size == erh->flat_size);

    tupdesc = expanded_record_get_tupdesc(erh);

    // Zero-fill all padding for consistent binary representation
    memset(tuphdr, 0, allocated_size);

    // Set up header fields
    HeapTupleHeaderSetDatumLength(tuphdr, allocated_size);
    HeapTupleHeaderSetTypeId(tuphdr, erh->er_typeid);
    HeapTupleHeaderSetTypMod(tuphdr, erh->er_typmod);
    ItemPointerSetInvalid(&(tuphdr->t_ctid));
    HeapTupleHeaderSetNatts(tuphdr, tupdesc->natts);
    tuphdr->t_hoff = erh->hoff;

    // Fill data area from dvalues/dnulls
    heap_fill_tuple(tupdesc, erh->dvalues, erh->dnulls,
                   (char *) tuphdr + erh->hoff, erh->data_len,
                   &tuphdr->t_infomask,
                   (erh->hasnull ? tuphdr->t_bits : NULL));
}
```