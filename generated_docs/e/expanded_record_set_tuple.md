# expanded_record_set_tuple

## Location
[src/backend/utils/adt/expandedrecord.c:440-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L440-L579)

## Overview
Sets the tuple value of an expanded record, handling copying, external field detoasting, domain constraint checking, and proper memory management of old and new tuple data.

## Definition
```c
void expanded_record_set_tuple(ExpandedRecordHeader *erh, HeapTuple tuple, bool copy, bool expand_external)
```

## Detailed Description
This function assigns a tuple as the value of an expanded record, providing comprehensive control over memory management and data processing. It supports both copying the tuple into local storage or referencing the original tuple, with automatic detoasting of out-of-line field values when requested.

The function performs domain constraint validation when the expanded record represents a domain type, ensuring the new tuple satisfies all domain constraints. It carefully manages memory by tracking allocation flags, properly freeing old tuple storage, and handling the transition between different tuple representations. The function maintains the expanded record's validity even in case of partial failures by updating flags appropriately.

## Parameters / Member Variables
- `erh`: The ExpandedRecordHeader to modify
- `tuple`: The HeapTuple to assign as the record's value (can be NULL to create an empty record)
- `copy`: Whether to physically copy the tuple into local storage (true) or reference the original (false)
- `expand_external`: Whether to inline out-of-line field values (true) or leave them as-is (false)

## Dependencies
- Functions called/Symbols referenced:
  - [check_domain_for_new_tuple](../c/check_domain_for_new_tuple.md)
  - HeapTupleHasExternal
  - [get_short_term_cxt](../g/get_short_term_cxt.md)
  - [toast_flatten_tuple](../t/toast_flatten_tuple.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - TupleDescAttr
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [pfree](../p/pfree.md)
- Called from:
  - (No direct references found in the analyzed codebase)

## Notes and Other Information
- The combination copy = false, expand_external = true is not supported and will trigger an assertion failure
- Domain constraints are checked before performing the assignment to ensure validity
- Properly handles cleanup of old field values, distinguishing between locally allocated and externally managed storage
- Uses short-term memory context for detoasting operations to avoid memory leaks
- Updates ER_FLAG_FVALUE_VALID, ER_FLAG_FVALUE_ALLOCED, and ER_FLAG_HAVE_EXTERNAL flags as appropriate
- Resets flat_size information which will be recalculated when needed
- Invalidates any existing deconstructed representation (ER_FLAG_DVALUES_VALID is cleared)
- Can handle NULL tuple assignments to create logically empty records