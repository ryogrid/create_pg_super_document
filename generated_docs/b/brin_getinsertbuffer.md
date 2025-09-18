# brin_getinsertbuffer

## Location
src/backend/access/brin/brin_pageops.c: 690 - 883

## Overview
Returns a pinned and exclusively locked buffer suitable for inserting a BRIN index item, handling page allocation, extension, and locking coordination with existing buffers.

## Definition
```c
static Buffer brin_getinsertbuffer(Relation irel, Buffer oldbuf, Size itemsz,
                                   bool *extended)
```

## Detailed Description
This function is the core buffer management routine for BRIN index insertions, responsible for finding or creating a suitable page with enough free space for a new index item. It implements sophisticated logic to handle multiple scenarios including page reuse, relation extension, and coordination between old and new buffers.

The function operates through several key phases:

1. **Target Page Selection**: Uses the relation's target block hint or consults the Free Space Map (FSM) to find a candidate page
2. **Buffer Acquisition Loop**: Iteratively attempts to find a suitable buffer until successful
3. **Locking Coordination**: Implements deadlock-avoidance by locking buffers in block number order
4. **Revmap Detection**: Checks if pages have been converted to revmap pages during concurrent operations
5. **Extension Handling**: Extends the relation when no existing page has sufficient space
6. **Space Validation**: Verifies that the selected page actually has enough free space

The function handles several complex scenarios:
- **Concurrent revmap extension**: Detects when pages are converted to revmap use and returns InvalidBuffer
- **FSM inconsistencies**: Updates FSM when pages don't have expected free space
- **Relation extension**: Creates new pages when needed, with proper initialization and FSM updates
- **Buffer coordination**: Manages locking order between old and new buffers to prevent deadlocks

## Parameters / Member Variables
- `irel`: Relation structure representing the BRIN index
- `oldbuf`: Existing buffer that may also need locking (can be InvalidBuffer)
- `itemsz`: Size of the item to be inserted, must not exceed BrinMaxItemSize
- `extended`: Output parameter set to true if the relation was extended to create a new page

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (to get block numbers from buffers)
  - RelationGetTargetBlock/RelationSetTargetBlock (for target block management)
  - GetPageWithFreeSpace (to query FSM for pages with free space)
  - [ReadBuffer](../R/ReadBuffer.md) (to read pages into buffers)
  - [LockBuffer](../L/LockBuffer.md)/UnlockReleaseBuffer (for buffer locking)
  - LockRelationForExtension/UnlockRelationForExtension (for extension coordination)
  - [BufferGetPage](../B/BufferGetPage.md) (to access page data)
  - [br_page_get_freespace](br_page_get_freespace.md) (to measure available space)
  - [brin_initialize_empty_new_buffer](brin_initialize_empty_new_buffer.md) (to initialize new pages)
  - RecordAndGetPageWithFreeSpace (to update FSM and find new pages)
  - FreeSpaceMapVacuumRange (to update FSM for extended pages)
  - BRIN_IS_REGULAR_PAGE (to check page type)
  - RELATION_IS_LOCAL (to check relation locality)
  - BrinMaxItemSize (maximum item size constant)
  - P_NEW (special block number for extension)
  - Various lock and buffer constants
- Called from:
  - [brin_doupdate](brin_doupdate.md) (for tuple updates)
  - [brin_doinsert](brin_doinsert.md) (for tuple insertions)

## Notes and Other Information
- This is a static function internal to brin_pageops.c
- Implements deadlock avoidance by always locking buffers in ascending block number order
- The caller is responsible for initializing extended pages and updating FSM after insertion
- Handles the corner case where FSM suggests a page that has been converted to revmap use
- May extend the relation but not return the new page if concurrent revmap extension occurs
- The function ensures that extended pages are properly initialized and recorded in FSM even when not returned
- Contains detailed error handling for oversized items that exceed the maximum page capacity
- Uses CHECK_FOR_INTERRUPTS to allow cancellation during potentially long-running operations
- The extension lock is held only during the critical section of relation extension to minimize contention
- Returns InvalidBuffer when the old buffer is found to be converted to a revmap page, signaling the caller to restart the operation