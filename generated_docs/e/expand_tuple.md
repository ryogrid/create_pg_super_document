# expand_tuple

## Location
[src/backend/access/common/heaptuple.c:828-1052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L828-L1052)

## Overview
Internal static function that expands a tuple with fewer attributes to a target tuple descriptor by filling in missing values or NULLs for absent attributes.

## Definition


## Detailed Description
The  function is a core internal function that handles tuple expansion when a source tuple has fewer attributes than required by a target tuple descriptor. This situation commonly occurs during schema evolution when new columns are added to tables and existing tuples need to be logically expanded to match the new schema.

The function can create either a HeapTuple or MinimalTuple as output (exactly one target parameter must be non-NULL). For missing attributes, it uses default values from the tuple descriptor's constraint information if available, otherwise it sets them to NULL. The function handles complex memory layout calculations including null bitmap management, data alignment, and proper tuple header initialization.

The expansion process involves calculating the required memory size, allocating and initializing the target tuple structure, copying existing attribute data, and filling in missing attributes with appropriate values or NULLs.

## Parameters / Member Variables
- : Pointer to HeapTuple pointer for output (NULL if creating MinimalTuple)
- : Pointer to MinimalTuple pointer for output (NULL if creating HeapTuple)
- : The source HeapTuple with fewer attributes that needs expansion
- : Tuple descriptor defining the target schema with more attributes

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHasNulls (null bitmap checking)
  - HeapTupleHeaderGetNatts (attribute count extraction)
  - BITMAPLEN (null bitmap size calculation)
  - att_align_datum (data alignment)
  - att_addlength_pointer (length calculation)
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - HeapTupleHeaderSetNatts (attribute count setting)
  - HeapTupleHeaderSetDatumLength (length setting)
  - HeapTupleHeaderSetTypeId (type ID setting)
  - HeapTupleHeaderSetTypMod (type modifier setting)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md) (tuple ID initialization)
  - [fill_val](../f/fill_val.md) (attribute value filling)
- Called from (representative examples):
  - [minimal_expand_tuple](../m/minimal_expand_tuple.md)
  - [heap_expand_tuple](../h/heap_expand_tuple.md)

## Notes and Other Information
- Static function - not part of the public API
- Requires exactly one of targetHeapTuple or targetMinimalTuple to be non-NULL
- Source tuple must have fewer attributes than the target tuple descriptor
- Handles complex memory layout with proper alignment and null bitmap management
- Uses AttrMissing information from tuple descriptor constraints for default values
- Critical for schema evolution and backward compatibility
- Located in src/backend/access/common/heaptuple.c:828-1052