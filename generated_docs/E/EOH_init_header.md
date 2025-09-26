# EOH_init_header

## Location
[src/backend/utils/adt/expandeddatum.c:48-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandeddatum.c#L48-L74)

## Overview
Initializes the common header fields of an expanded object, setting up the TOAST pointers and basic metadata required for expanded object functionality.

## Definition

```c
void
EOH_init_header(ExpandedObjectHeader *eohptr,
				const ExpandedObjectMethods *methods,
				MemoryContext obj_context)
```
## Detailed Description
EOH_init_header is responsible for initializing the fundamental components of an ExpandedObjectHeader structure. The primary purpose is to set up the TOAST (The Oversized-Attribute Storage Technique) pointers that allow the expanded object to be referenced as both read-write and read-only variants.

The function sets the magic number that identifies this as an expanded object header, assigns the methods table that defines type-specific operations, and establishes the memory context for the object. It then creates both read-write and read-only external TOAST pointers that can be used to reference this expanded object from Datum values.

## Parameters / Member Variables
- : Pointer to the ExpandedObjectHeader structure to initialize
- : Pointer to the type-specific methods table that defines operations for this expanded object type  
- : Memory context in which the expanded object resides

## Dependencies
- Functions called/Symbols referenced:
  - SET_VARTAG_EXTERNAL (macro)
  - VARDATA_EXTERNAL (macro)
  - memcpy (standard library function)
- Constants referenced:
  - EOH_HEADER_MAGIC
  - VARTAG_EXPANDED_RW
  - VARTAG_EXPANDED_RO
- Types referenced:
  - [ExpandedObjectHeader](ExpandedObjectHeader.md)
  - [ExpandedObjectMethods](ExpandedObjectMethods.md)
  - [varatt_expanded](../v/varatt_expanded.md)
  - [MemoryContext](../M/MemoryContext.md)
- Called from (representative examples):
  - [expand_array](../e/expand_array.md)
  - [make_expanded_record_from_typeid](../m/make_expanded_record_from_typeid.md)
  - [make_expanded_record_from_tupdesc](../m/make_expanded_record_from_tupdesc.md)
  - [make_expanded_record_from_exprecord](../m/make_expanded_record_from_exprecord.md)
  - [make_expanded_record_from_datum](../m/make_expanded_record_from_datum.md)

## Notes and Other Information
- The function creates two TOAST pointers: one for read-write access (VARTAG_EXPANDED_RW) and one for read-only access (VARTAG_EXPANDED_RO)
- The EOH_HEADER_MAGIC value serves as a type identifier for expanded object headers
- This initialization is essential before an expanded object can be used in PostgreSQL's type system
- The function is defined in src/backend/utils/adt/expandeddatum.c:48-74