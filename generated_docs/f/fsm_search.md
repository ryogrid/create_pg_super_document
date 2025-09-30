# fsm_search

## Location
[src/backend/storage/freespace/freespace.c:678-811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L678-L811)

## Overview
Searches the FSM tree structure to find a heap page with at least the specified minimum amount of free space, handling inconsistencies and stale information through automatic correction and retry mechanisms.

## Definition
```c
static BlockNumber fsm_search(Relation rel, uint8 min_cat)
```

## Detailed Description
This function implements the core search algorithm for the Free Space Map, traversing the FSM tree from root to leaf to locate a heap page with sufficient free space. The search process is complex due to the need to handle concurrent updates, stale information, and boundary conditions.

The algorithm works as follows:
1. **Tree Traversal**: Starts at the FSM root and descends the tree level by level
2. **Page Search**: At each level, searches for a slot meeting the minimum space requirement
3. **Descent Logic**: If found and not at bottom level, descends to the child page
4. **Bottom Level**: At the bottom level, converts FSM slot to heap block number and validates existence
5. **Correction Mechanism**: When encountering stale information, updates parent pages and restarts from root
6. **Safety Limits**: Includes restart limiting to prevent infinite loops

The function handles several edge cases:
- Pages beyond the end of the relation (updates FSM and restarts)
- Stale upper-level information (corrects parent and restarts)
- Race conditions during concurrent updates
- Emergency brake for excessive restart loops

## Parameters / Member Variables
- `rel`: Relation to search for free space
- `min_cat`: Minimum free space category required (0-255 scale)

## Dependencies
- Functions called/Symbols referenced:
  - [fsm_readbuf](fsm_readbuf.md)
  - [fsm_search_avail](fsm_search_avail.md)
  - [fsm_get_max_avail](fsm_get_max_avail.md)
  - [fsm_get_heap_blk](fsm_get_heap_blk.md)
  - [fsm_does_block_exist](fsm_does_block_exist.md)
  - [fsm_set_avail](fsm_set_avail.md)
  - [fsm_get_child](fsm_get_child.md)
  - [fsm_get_parent](fsm_get_parent.md)
  - [fsm_set_and_search](fsm_set_and_search.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)
- Called from (representative examples):
  - [GetPageWithFreeSpace](../G/GetPageWithFreeSpace.md)
  - [RecordAndGetPageWithFreeSpace](../R/RecordAndGetPageWithFreeSpace.md)

## Notes and Other Information
- This is a static function, only accessible within the freespace.c file
- Implements a self-correcting search algorithm that handles stale FSM information
- Uses shared locks during search to allow concurrent readers
- Switches to exclusive locks only when updating stale information
- Includes protection against infinite loops with a 10,000 restart limit
- The function tolerates race conditions and eventual consistency through retry logic
- Performance is optimized by pinning buffers during descent when possible
- Handles the case where heap blocks referenced by FSM no longer exist
- The search is restart-based rather than backtrack-based for simplicity and correctness

## Simplified Source

```c
static BlockNumber fsm_search(Relation rel, uint8 min_cat) {
    int restarts = 0;
    FSMAddress addr = FSM_ROOT_ADDRESS;

    for (;;) {
        int slot;
        Buffer buf;
        uint8 max_avail = 0;

        // Read FSM page and search for available space
        buf = fsm_readbuf(rel, addr, false);

        if (BufferIsValid(buf)) {
            LockBuffer(buf, BUFFER_LOCK_SHARE);
            slot = fsm_search_avail(buf, min_cat,
                                  (addr.level == FSM_BOTTOM_LEVEL), false);

            if (slot == -1) {
                // No slot found - get max available space for parent update
                max_avail = fsm_get_max_avail(BufferGetPage(buf));
                UnlockReleaseBuffer(buf);
            } else {
                // Found slot - keep buffer pinned for potential update
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }
        } else {
            slot = -1;
        }

        if (slot != -1) {
            // Found suitable slot
            if (addr.level == FSM_BOTTOM_LEVEL) {
                // At bottom level - convert to heap block number
                BlockNumber blkno = fsm_get_heap_blk(addr, slot);

                if (fsm_does_block_exist(rel, blkno)) {
                    ReleaseBuffer(buf);
                    return blkno;  // Success!
                }

                // Block doesn't exist - clear FSM entry and restart
                Page page = BufferGetPage(buf);
                LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
                fsm_set_avail(page, slot, 0);
                MarkBufferDirtyHint(buf, false);
                UnlockReleaseBuffer(buf);

                if (restarts++ > 10000)
                    return InvalidBlockNumber;  // Safety limit
                addr = FSM_ROOT_ADDRESS;
            } else {
                // Descend to child level
                ReleaseBuffer(buf);
                addr = fsm_get_child(addr, slot);
            }
        } else if (addr.level == FSM_ROOT_LEVEL) {
            // At root with no space - give up
            return InvalidBlockNumber;
        } else {
            // Update parent with correct information and restart
            uint16 parentslot;
            FSMAddress parent = fsm_get_parent(addr, &parentslot);
            fsm_set_and_search(rel, parent, parentslot, max_avail, 0);

            if (restarts++ > 10000)
                return InvalidBlockNumber;  // Safety limit

            // Restart from root
            addr = FSM_ROOT_ADDRESS;
        }
    }
}
```