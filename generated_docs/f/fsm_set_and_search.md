# fsm_set_and_search

## Location
[src/backend/storage/freespace/freespace.c:646-677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L646-L677)

## Overview
Sets a free space value for a specific slot in an FSM page and optionally searches for a slot with at least the specified minimum free space, combining both operations under a single buffer lock.

## Definition
```c
static int fsm_set_and_search(Relation rel, FSMAddress addr, uint16 slot, uint8 newValue, uint8 minValue)
```

## Detailed Description
This function efficiently combines two common FSM operations - setting a free space value and searching for available space - into a single atomic operation. This design minimizes buffer lock overhead and ensures consistency between the set and search operations.

The function follows this sequence:
1. Reads the target FSM page using fsm_readbuf() with extension enabled
2. Acquires an exclusive buffer lock
3. Sets the new availability value for the specified slot using fsm_set_avail()
4. If the page was modified, marks it dirty with a hint (since FSM changes aren't WAL-logged)
5. If a minimum value search is requested (minValue > 0), searches the same page for a slot meeting the criteria
6. Releases the lock and returns the search result

The search operation, when performed, takes advantage of already holding the buffer lock and having the page in cache, making it very efficient.

## Parameters / Member Variables
- `rel`: Relation whose FSM page is being modified
- `addr`: FSMAddress identifying the target FSM page
- `slot`: Slot number within the page to update (0-based index)
- `newValue`: New free space availability value to set (0-255 scale)
- `minValue`: Minimum free space value to search for (0 means no search)

## Dependencies
- Functions called/Symbols referenced:
  - fsm_readbuf
  - LockBuffer
  - BufferGetPage
  - fsm_set_avail
  - MarkBufferDirtyHint
  - fsm_search_avail
  - UnlockReleaseBuffer
  - BUFFER_LOCK_EXCLUSIVE
  - FSM_BOTTOM_LEVEL
- Called from (representative examples):
  - RecordAndGetPageWithFreeSpace
  - RecordPageWithFreeSpace
  - fsm_search

## Notes and Other Information
- This is a static function, only accessible within the freespace.c file
- Combines set and search operations to minimize lock overhead
- Uses MarkBufferDirtyHint() since FSM changes are not WAL-logged
- Returns -1 if no slot meeting the minimum value criteria is found, otherwise returns the slot number
- The search operation respects whether the page is at the bottom level of the FSM tree
- Automatically extends the FSM file if the target page doesn't exist
- The function ensures atomicity of both operations under a single exclusive lock