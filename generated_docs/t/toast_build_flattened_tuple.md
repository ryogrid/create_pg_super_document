# toast_build_flattened_tuple

## Location
[src/backend/access/heap/heaptoast.c:563-625](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heaptoast.c#L563-L625)

## Overview
Builds a heap tuple from Datum arrays while expanding any external TOAST pointers to create a tuple with no out-of-line references.

## Definition
```c
HeapTuple toast_build_flattened_tuple(TupleDesc tupleDesc, Datum *values, bool *isnull)
```

## Detailed Description
The `toast_build_flattened_tuple` function is essentially a variant of `heap_form_tuple` that ensures the resulting tuple contains no external TOAST references. It processes the input Datum array, identifying any externally stored values and retrieving their full content using `detoast_external_attr` before constructing the final tuple.

This function is particularly useful when constructing tuples that need to be self-contained, such as when building result tuples for certain operations where external TOAST access might not be available or efficient. Unlike other flattening functions, it operates on separate values and isnull arrays rather than an existing tuple.

The function preserves the caller's isnull array unchanged but creates a modified copy of the values array to handle detoasted values. It carefully manages memory by tracking which values need to be freed after tuple construction.

## Parameters / Member Variables
- `tupleDesc`: The tuple descriptor describing the target tuple structure
- `values`: Array of Datum values to be included in the tuple
- `isnull`: Array of null indicators corresponding to the values

## Dependencies
- Functions called/Symbols referenced:
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - VARATT_IS_EXTERNAL
  - MaxTupleAttributeNumber
- Called from (representative examples):
  - [ExecEvalWholeRowVar](../E/ExecEvalWholeRowVar.md)

## Notes and Other Information
- Similar to `heap_form_tuple` but with automatic expansion of external TOAST references
- Does not decompress inline compressed datums or modify short-header values
- Preserves the caller's isnull array unchanged while modifying a copy of the values array
- Implements careful memory management by tracking and freeing temporary detoasted values
- Useful for creating self-contained tuples without external dependencies
- The question of whether to also decompress inline compressed datums remains unresolved (currently they are left compressed)
- Part of PostgreSQL's TOAST system for handling oversized attribute values

## Simplified Source

```c
HeapTuple toast_build_flattened_tuple(TupleDesc tupleDesc, Datum *values, bool *isnull)
{
    HeapTuple new_tuple;
    int numAttrs = tupleDesc->natts;
    int num_to_free = 0;
    Datum new_values[MaxTupleAttributeNumber];
    Pointer freeable_values[MaxTupleAttributeNumber];

    // Copy input values array (isnull array used directly)
    Assert(numAttrs <= MaxTupleAttributeNumber);
    memcpy(new_values, values, numAttrs * sizeof(Datum));

    // Process each attribute to expand external TOAST values
    for (int i = 0; i < numAttrs; i++)
    {
        // Check non-null variable-length attributes
        if (!isnull[i] && TupleDescAttr(tupleDesc, i)->attlen == -1)
        {
            struct varlena *new_value = (struct varlena *) DatumGetPointer(new_values[i]);

            // If value is stored externally, retrieve full content
            if (VARATT_IS_EXTERNAL(new_value))
            {
                new_value = detoast_external_attr(new_value);
                new_values[i] = PointerGetDatum(new_value);
                freeable_values[num_to_free++] = (Pointer) new_value;
            }
        }
    }

    // Build tuple with flattened values
    new_tuple = heap_form_tuple(tupleDesc, new_values, isnull);

    // Clean up temporary detoasted values
    for (int i = 0; i < num_to_free; i++)
        pfree(freeable_values[i]);

    return new_tuple;
}
```