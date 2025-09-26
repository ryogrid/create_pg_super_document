# EOH_get_flat_size

## Location
[src/backend/utils/adt/expandeddatum.c:75-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandeddatum.c#L75-L80)

## Overview
A convenience function that invokes the get_flat_size method of an expanded object to determine the size needed for its flattened representation.

## Definition

```c
Size
EOH_get_flat_size(ExpandedObjectHeader *eohptr)
```
## Detailed Description
EOH_get_flat_size is a simple wrapper function that provides a convenient interface for calling the type-specific get_flat_size method of an expanded object. This method calculates the size in bytes that would be required to store the expanded object in its flattened (non-expanded) form.

The function delegates to the appropriate type-specific implementation through the expanded object's method table (eoh_methods), ensuring that each expanded object type can define its own size calculation logic based on its internal structure and data.

## Parameters / Member Variables
- : Pointer to the ExpandedObjectHeader whose flattened size is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - get_flat_size (method from ExpandedObjectMethods table)
- Types referenced:
  - [ExpandedObjectHeader](ExpandedObjectHeader.md)
  - Size
- Called from (representative examples):
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [toast_raw_datum_size](../t/toast_raw_datum_size.md)
  - [toast_datum_size](../t/toast_datum_size.md)
  - [heap_compute_data_size](../h/heap_compute_data_size.md)
  - [datumCopy](../d/datumCopy.md)
  - [datumEstimateSpace](../d/datumEstimateSpace.md)

## Notes and Other Information
- This is a polymorphic function that relies on the object's method table for the actual implementation
- The size returned represents the space needed for the flattened form, not the current expanded form
- Used extensively in TOAST operations and datum serialization/copying operations
- Part of the expanded object API that provides type-agnostic access to type-specific functionality
- The function is defined in src/backend/utils/adt/expandeddatum.c:75-80