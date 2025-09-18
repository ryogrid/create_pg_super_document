# varatt_indirect

## Location
src/include/varatt.h: 57 - 60

## Overview
A structure representing a "TOAST pointer" for out-of-line Datum stored in memory rather than in an external TOAST relation.

## Definition


## Detailed Description
The varatt_indirect structure is a specialized type of TOAST pointer used for managing out-of-line data that resides in memory rather than being stored in an external TOAST table. Unlike varatt_external which points to data in a TOAST relation on disk, varatt_indirect directly references in-memory varlena data structures.

This mechanism is particularly useful for temporary data or data that needs to be kept in memory for performance reasons. The creator of such a Datum bears full responsibility for ensuring that the referenced storage remains valid for as long as any referencing pointer Datums can exist, as there is no automatic garbage collection or reference counting.

Like other TOAST pointer structures, varatt_indirect is stored unaligned within containing tuples, requiring careful handling when accessing its fields.

## Parameters / Member Variables
- : A direct pointer to an in-memory varlena structure containing the actual data

## Dependencies
- Functions called/Symbols referenced:
  - [varlena](varlena.md) (PostgreSQL variable-length data structure)
- Called from (representative examples):
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [detoast_attr](../d/detoast_attr.md)
  - [detoast_attr_slice](../d/detoast_attr_slice.md)
  - [toast_raw_datum_size](../t/toast_raw_datum_size.md)
  - [toast_datum_size](../t/toast_datum_size.md)
  - [ReorderBufferToastReplace](../R/ReorderBufferToastReplace.md)
  - [make_tuple_indirect](../m/make_tuple_indirect.md)

## Notes and Other Information
- Used for out-of-line data stored in memory rather than on disk
- The creator is responsible for memory management and ensuring storage validity
- Stored unaligned within tuples, requiring memcpy for safe field access
- Provides a mechanism for efficient handling of large temporary data structures
- Part of PostgreSQL's flexible TOAST system that can handle both disk-based and memory-based out-of-line storage
- Must be used with caution due to manual memory management requirements