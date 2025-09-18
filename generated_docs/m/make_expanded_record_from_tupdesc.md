# make_expanded_record_from_tupdesc

## Location
src/backend/utils/adt/expandedrecord.c: 205 - 328

## Overview
Creates an expanded record object from a given TupleDesc, copying the tupdesc if necessary or incrementing its reference count when possible.

## Definition
```c
ExpandedRecordHeader *make_expanded_record_from_tupdesc(TupleDesc tupdesc, MemoryContext parentcontext)
```

## Detailed Description
This function builds an expanded record based on the rowtype defined by the provided TupleDesc. It intelligently handles tuple descriptor management by preferring to reference the type cache's copy for named composite types (which guarantees reference counting) while copying the tupdesc when necessary for other cases.

For named composite types (non-RECORD types), the function consults the type cache to obtain the canonical refcounted version of the tuple descriptor and the correct tupdesc identifier. For RECORD types, it assigns a unique identifier while using the provided tupdesc. The resulting expanded record is initialized in an "empty" state logically equivalent to a NULL composite value.

## Parameters / Member Variables
- `tupdesc`: The TupleDesc defining the structure of the record to be created
- `parentcontext`: Memory context that will be the parent of the expanded object's private context

## Dependencies
- Functions called/Symbols referenced:
  - lookup_type_cache
  - assign_record_type_identifier
  - AllocSetContextCreate
  - MemoryContextAlloc
  - EOH_init_header
  - MemoryContextRegisterResetCallback
  - CreateTupleDescCopy
  - MemoryContextSwitchTo
- Called from:
  - (No direct references found in the analyzed codebase)

## Notes and Other Information
- Prefers to use type cache copies of tuple descriptors for named composite types to ensure proper reference counting
- Automatically handles both refcounted and non-refcounted tuple descriptors appropriately
- For refcounted tupdescs, uses memory context callbacks to manage reference counting lifecycle
- For non-refcounted tupdescs, creates a private copy using CreateTupleDescCopy and sets ER_FLAG_TUPDESC_ALLOCED
- Uses regular-size memory context to improve odds of fitting tuple descriptors without extra allocations
- The resulting record does not have field validity flags set, maintaining the "empty" state until explicitly populated