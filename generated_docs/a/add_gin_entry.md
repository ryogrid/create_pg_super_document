# add_gin_entry

## Location
[src/backend/utils/adt/jsonb_gin.c:172-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L172-L202)

## Overview
Adds a new Datum entry to a GinEntries buffer, automatically resizing the buffer when necessary and returning the index of the added entry.

## Definition


## Detailed Description
This static function manages the dynamic growth of a GinEntries buffer by adding new Datum entries to it. The function implements an automatic buffer expansion strategy: when the buffer reaches capacity, it either doubles the current allocation size (if previously allocated) or starts with an initial allocation of 8 entries. This approach balances memory efficiency with performance by reducing the frequency of memory reallocations as the buffer grows.

The function is essential for building GIN index entries during JSONB processing, where the number of entries to be extracted is not known in advance. It provides a clean interface for incrementally building the entry collection while managing memory allocation internally.

## Parameters / Member Variables
- : Pointer to the GinEntries structure to add the entry to
- : The Datum value to be added to the buffer

## Return Value
- Returns the index (ID) of the newly added entry within the buffer (0-based)

## Dependencies
- Functions called/Symbols referenced:
  - GinEntries (struct type)
  - [repalloc](../r/repalloc.md) (memory reallocation function)
  - [palloc](../p/palloc.md) (memory allocation function)
- Called from (representative examples):
  - [gin_extract_jsonb](../g/gin_extract_jsonb.md) (at src/backend/utils/adt/jsonb_gin.c:256, 260, 263)
  - [emit_jsp_gin_entries](../e/emit_jsp_gin_entries.md) (at src/backend/utils/adt/jsonb_gin.c:727)
  - [gin_extract_jsonb_path](../g/gin_extract_jsonb_path.md) (at src/backend/utils/adt/jsonb_gin.c:1153)

## Notes and Other Information
- This is a static function, accessible only within the jsonb_gin.c file
- Uses an exponential growth strategy (doubling) for buffer expansion to achieve amortized O(1) insertion time
- The initial allocation size is 8 entries when starting from an unallocated buffer
- Memory operations use PostgreSQL's palloc/repalloc functions for proper memory context integration
- The returned index can be used to reference the entry later, though this functionality appears to be primarily for tracking purposes