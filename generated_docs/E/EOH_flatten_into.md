# EOH_flatten_into

## Location
[src/backend/utils/adt/expandeddatum.c:81-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandeddatum.c#L81-L94)

## Overview
A convenience function that invokes the flatten_into method of an expanded object to convert it from its expanded form into its flattened representation in a provided buffer.

## Definition

```c
void
EOH_flatten_into(ExpandedObjectHeader *eohptr,
				 void *result, Size allocated_size)
```
## Detailed Description
EOH_flatten_into is a wrapper function that provides a convenient interface for calling the type-specific flatten_into method of an expanded object. This method converts the expanded object from its in-memory expanded representation back to its flattened (disk/wire) format, storing the result in the provided buffer.

The function delegates to the appropriate type-specific implementation through the expanded object's method table (eoh_methods), allowing each expanded object type to define its own flattening logic based on its internal structure. The caller is responsible for providing a buffer of sufficient size, typically determined by calling EOH_get_flat_size first.

## Parameters / Member Variables
- : Pointer to the ExpandedObjectHeader to be flattened
- : Pointer to the buffer where the flattened representation should be stored
- : Size of the allocated buffer (used for validation/assertions by implementations)

## Dependencies
- Functions called/Symbols referenced:
  - flatten_into (method from ExpandedObjectMethods table)
- Types referenced:
  - [ExpandedObjectHeader](ExpandedObjectHeader.md)
  - Size
- Called from (representative examples):
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [fill_val](../f/fill_val.md)
  - [tts_virtual_materialize](../t/tts_virtual_materialize.md)
  - [datumCopy](../d/datumCopy.md)
  - [datumSerialize](../d/datumSerialize.md)

## Notes and Other Information
- This is a polymorphic function that relies on the object's method table for the actual implementation
- The caller must ensure the result buffer is large enough to hold the flattened representation
- Typically used in conjunction with EOH_get_flat_size to determine the required buffer size
- Part of the expanded object API that enables conversion between expanded and flattened representations
- Used in TOAST operations, tuple materialization, and datum serialization
- The function is defined in src/backend/utils/adt/expandeddatum.c:81-94