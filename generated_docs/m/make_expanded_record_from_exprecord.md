# make_expanded_record_from_exprecord

## Location
src/backend/utils/adt/expandedrecord.c: 329 - 439

## Overview
Creates a new expanded record with the same rowtype as an existing expanded record, optimized by bypassing type cache lookups and copying only type metadata.

## Definition
```c
ExpandedRecordHeader *make_expanded_record_from_exprecord(ExpandedRecordHeader *olderh, MemoryContext parentcontext)
```

## Detailed Description
This function builds an expanded record of the same rowtype as the given expanded record, providing a performance optimization over the other creation methods by avoiding type cache lookups. It copies type identification information and tuple descriptor management strategy from the source expanded record, but creates a completely new, empty record instance.

The function intelligently handles tuple descriptor sharing based on the source record's management approach: using reference counting for refcounted descriptors, copying when the source has a private copy, or assuming persistence when the source uses a shared descriptor. The resulting record inherits only the IS_DOMAIN flag from the source, with all other state flags reset to maintain the empty initialization.

## Parameters / Member Variables
- `olderh`: The existing ExpandedRecordHeader to copy the rowtype structure from
- `parentcontext`: Memory context that will be the parent of the new expanded object's private context

## Dependencies
- Functions called/Symbols referenced:
  - expanded_record_get_tupdesc
  - AllocSetContextCreate
  - MemoryContextAlloc
  - EOH_init_header
  - MemoryContextRegisterResetCallback
  - CreateTupleDescCopy
  - MemoryContextSwitchTo
- Called from:
  - (No direct references found in the analyzed codebase)

## Notes and Other Information
- Provides significant performance benefits by avoiding type cache lookups that the other creation functions require
- Does not copy any tuple data from the source expanded record, only the structural type information
- Inherits only the ER_FLAG_IS_DOMAIN flag from the source record while resetting all other state flags
- Uses the same tuple descriptor management strategy as the source record (refcounting, copying, or sharing)
- The new record is initialized in an empty state without setting ER_FLAG_DVALUES_VALID or ER_FLAG_FVALUE_VALID
- Optimizes memory allocation by pre-allocating dvalues/dnulls arrays alongside the header structure