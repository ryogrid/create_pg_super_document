# deconstruct_expanded_array

## Location
[src/backend/utils/adt/array_expanded.c:424-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L424-L453)

## Overview
deconstruct_expanded_array populates the Datum/isnull representation fields of an expanded array header if they haven't been created previously, enabling element-wise access to the array data.

## Definition
```c
void deconstruct_expanded_array(ExpandedArrayHeader *eah)
```

## Detailed Description
This function ensures that an expanded array has its individual elements available as separate Datum values with corresponding null flags. It checks if the dvalues field is NULL, indicating that the Datum representation hasn't been created yet. If so, it switches to the expanded array's memory context and calls deconstruct_array() to break down the flat array representation (fvalue) into individual Datum values and null flags.

The function is designed with error safety in mind - it only updates the header fields after successful completion of the deconstruction process. This prevents partial state corruption if deconstruct_array() fails partway through. The resulting deconstructed representation provides efficient element-wise access for operations that need to work with individual array elements rather than the flat array format.

## Parameters / Member Variables
- `eah`: Pointer to the ExpandedArrayHeader whose Datum representation should be created

## Dependencies
- Functions called/Symbols referenced:
  - [deconstruct_array](deconstruct_array.md) (breaks flat array into individual Datum values)
  - ARR_HASNULL (checks if array contains null elements)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (manages memory allocation context)
- Called from (representative examples):
  - [statext_expressions_load](../s/statext_expressions_load.md) (statistics processing)
  - [array_get_element_expanded](../a/array_get_element_expanded.md) (element access operations)
  - [array_set_element_expanded](../a/array_set_element_expanded.md) (element modification operations)
  - [array_contain_compare](../a/array_contain_compare.md) (array comparison operations)

## Notes and Other Information
- The function is idempotent - calling it multiple times on the same expanded array header has no additional effect
- Memory allocation occurs in the expanded array's own memory context (eah->hdr.eoh_context)
- The dnulls array is only allocated if the source array actually contains null elements (checked via ARR_HASNULL)
- Updates to the header fields (dvalues, dnulls, dvalueslen, nelems) happen atomically after successful deconstruction
- This lazy initialization approach saves memory and processing time when element-wise access isn't needed
- The deconstructed representation coexists with the flat representation, providing different access patterns for different use cases

## Simplified Source

```c
void deconstruct_expanded_array(ExpandedArrayHeader *eah) {
    // Only create Datum representation if not already done
    if (eah->dvalues == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(eah->hdr.eoh_context);
        Datum *dvalues;
        bool *dnulls;
        int nelems;

        dnulls = NULL;

        // Break down flat array into individual Datum values
        deconstruct_array(eah->fvalue,
                         eah->element_type,
                         eah->typlen, eah->typbyval, eah->typalign,
                         &dvalues,
                         ARR_HASNULL(eah->fvalue) ? &dnulls : NULL,
                         &nelems);

        // Update header only after successful completion
        // This ensures atomicity and prevents partial corruption
        eah->dvalues = dvalues;
        eah->dnulls = dnulls;
        eah->dvalueslen = eah->nelems = nelems;

        MemoryContextSwitchTo(oldcxt);
    }
}
```