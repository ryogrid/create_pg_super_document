# toast_flatten_tuple

## Location
[src/backend/access/heap/heaptoast.c:350-448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heaptoast.c#L350-L448)

## Overview
Flattens a heap tuple by detoasting all out-of-line external attributes, creating a new tuple with all data stored inline.

## Definition
```c
HeapTuple toast_flatten_tuple(HeapTuple tup, TupleDesc tupleDesc)
```

## Detailed Description
The `toast_flatten_tuple` function creates a fully flattened version of a heap tuple by retrieving all externally stored (out-of-line) TOAST values and incorporating them into a new tuple. This process ensures that the resulting tuple contains no external references and can be safely used in contexts where external TOAST access might not be available or desired.

The function processes each variable-length attribute in the tuple, checking if it is externally stored using `VARATT_IS_EXTERNAL`. For external values, it calls `detoast_external_attr` to retrieve the full value from TOAST storage. The function preserves all tuple metadata including identity fields, visibility information, and transaction-related info masks.

Note that this function only handles out-of-line external values - it does not decompress inline compressed values or expand short-header datums, leaving those optimizations intact.

## Parameters / Member Variables
- `tup`: The heap tuple to be flattened (must have external attributes)
- `tupleDesc`: The tuple descriptor describing the tuple structure

## Dependencies
- Functions called/Symbols referenced:
  - [heap_deform_tuple](../h/heap_deform_tuple.md)
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - VARATT_IS_EXTERNAL
  - MaxTupleAttributeNumber
  - HEAP_XACT_MASK
  - HEAP2_XACT_MASK
- Called from (representative examples):
  - [ExtractReplicaIdentity](../E/ExtractReplicaIdentity.md)
  - [expanded_record_set_tuple](../e/expanded_record_set_tuple.md)
  - [CatalogCacheCreateEntry](../C/CatalogCacheCreateEntry.md)

## Notes and Other Information
- Expects the caller to have already verified that the tuple has external attributes using HeapTupleHasExternal()
- Does not eliminate compressed or short-header datums, only external references
- Preserves tuple identity fields (t_self, t_tableOid) and visibility information
- Carefully maintains transaction-related info masks from the original tuple
- Memory management: allocates new storage for detoasted values and cleans up temporary allocations
- The resulting tuple is completely self-contained with no external dependencies

## Simplified Source

```c
HeapTuple
toast_flatten_tuple(HeapTuple tup, TupleDesc tupleDesc)
{
    HeapTuple new_tuple;
    int numAttrs = tupleDesc->natts;
    int i;
    Datum toast_values[MaxTupleAttributeNumber];
    bool toast_isnull[MaxTupleAttributeNumber];
    bool toast_free[MaxTupleAttributeNumber];

    // Break down the tuple into individual attribute values
    Assert(numAttrs <= MaxTupleAttributeNumber);
    heap_deform_tuple(tup, tupleDesc, toast_values, toast_isnull);

    memset(toast_free, 0, numAttrs * sizeof(bool));

    // Process each variable-length attribute
    for (i = 0; i < numAttrs; i++) {
        // Check non-null varlena attributes for external storage
        if (!toast_isnull[i] && TupleDescAttr(tupleDesc, i)->attlen == -1) {
            struct varlena *new_value;

            new_value = (struct varlena *) DatumGetPointer(toast_values[i]);
            if (VARATT_IS_EXTERNAL(new_value)) {
                // Detoast external attribute and mark for cleanup
                new_value = detoast_external_attr(new_value);
                toast_values[i] = PointerGetDatum(new_value);
                toast_free[i] = true;
            }
        }
    }

    // Create new tuple with flattened values
    new_tuple = heap_form_tuple(tupleDesc, toast_values, toast_isnull);

    // Copy tuple identity and visibility information
    new_tuple->t_self = tup->t_self;
    new_tuple->t_tableOid = tup->t_tableOid;
    new_tuple->t_data->t_choice = tup->t_data->t_choice;
    new_tuple->t_data->t_ctid = tup->t_data->t_ctid;

    // Preserve transaction-related info masks
    new_tuple->t_data->t_infomask &= ~HEAP_XACT_MASK;
    new_tuple->t_data->t_infomask |= tup->t_data->t_infomask & HEAP_XACT_MASK;
    new_tuple->t_data->t_infomask2 &= ~HEAP2_XACT_MASK;
    new_tuple->t_data->t_infomask2 |= tup->t_data->t_infomask2 & HEAP2_XACT_MASK;

    // Clean up temporary allocations
    for (i = 0; i < numAttrs; i++)
        if (toast_free[i])
            pfree(DatumGetPointer(toast_values[i]));

    return new_tuple;
}
```