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
  - expanded_record_get_tupdesc
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