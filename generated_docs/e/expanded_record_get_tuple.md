# expanded_record_get_tuple

## Location
src/backend/utils/adt/expandedrecord.c: 884 - 901

## Overview
expanded_record_get_tuple returns a HeapTuple representation of an expanded records current value, using cached tuple when available or constructing one from field data.

## Definition
HeapTuple expanded_record_get_tuple(ExpandedRecordHeader *erh)

## Detailed Description
This function provides access to an expanded records data in HeapTuple format through three distinct code paths:

1. **Fast path (ER_FLAG_FVALUE_VALID)**: Returns the cached original tuple directly when it is still valid and unmodified
2. **Reconstruction path (ER_FLAG_DVALUES_VALID)**: Creates a new HeapTuple by assembling data from the expanded records dvalues and dnulls arrays
3. **Empty record path**: Returns NULL when the expanded record contains no data

The function is optimized to avoid unnecessary tuple construction when possible. However, the returned tuple may contain out-of-line toasted values and therefore is not suitable for use as a composite datum without further processing.

## Parameters / Member Variables
- `erh`: Pointer to the ExpandedRecordHeader from which to retrieve the tuple representation

## Dependencies
- Functions called/Symbols referenced:
  - [heap_form_tuple](../h/heap_form_tuple.md)
- Called from (representative examples):
  - No direct references found (likely used by external code accessing expanded records)

## Notes and Other Information
- The returned tuple should not be modified by the caller when it comes from the fast path (original cached tuple)
- Tuples returned from the reconstruction path are allocated in the current memory context
- Out-of-line toasted values are not inlined, making the result unsuitable as a composite datum
- Returns NULL for empty expanded records (neither ER_FLAG_FVALUE_VALID nor ER_FLAG_DVALUES_VALID are set)
- This function provides a bridge between the expanded object system and traditional HeapTuple-based code
- Part of PostgreSQL's expanded object infrastructure for efficient composite data manipulation