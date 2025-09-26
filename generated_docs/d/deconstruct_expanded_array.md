# deconstruct_expanded_array

## Location
src/backend/utils/adt/array_expanded.c: 424 - 453

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
  - deconstruct_array (breaks flat array into individual Datum values)
  - ARR_HASNULL (checks if array contains null elements)
  - MemoryContextSwitchTo (manages memory allocation context)
- Called from (representative examples):
  - statext_expressions_load (statistics processing)
  - array_get_element_expanded (element access operations)
  - array_set_element_expanded (element modification operations)
  - array_contain_compare (array comparison operations)

## Notes and Other Information
- The function is idempotent - calling it multiple times on the same expanded array header has no additional effect
- Memory allocation occurs in the expanded array's own memory context (eah->hdr.eoh_context)
- The dnulls array is only allocated if the source array actually contains null elements (checked via ARR_HASNULL)
- Updates to the header fields (dvalues, dnulls, dvalueslen, nelems) happen atomically after successful deconstruction
- This lazy initialization approach saves memory and processing time when element-wise access isn't needed
- The deconstructed representation coexists with the flat representation, providing different access patterns for different use cases