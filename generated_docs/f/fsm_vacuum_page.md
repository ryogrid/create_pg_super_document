# fsm_vacuum_page

## Location
src/backend/storage/freespace/freespace.c: 812 - 925

## Overview
Recursive function that examines FSM (Free Space Map) pages and their children, updating upper-level nodes that cover a specified heap block range during vacuum operations.

## Definition

```c
static uint8
fsm_vacuum_page(Relation rel, FSMAddress addr,
				BlockNumber start, BlockNumber end,
				bool *eof_p)
```
## Detailed Description
This function serves as the recursive core of the Free Space Map vacuum process. It performs a depth-first traversal of the FSM tree structure, examining each page and its children to update freespace information. The function operates on a specific FSM page identified by the address parameter and processes heap blocks within the specified range from start to end-1.

The function handles two main cases:
1. **Non-leaf pages**: Recursively processes child pages and updates the current page's slot information based on the maximum available space reported by children
2. **Leaf pages**: Simply returns the maximum available space on the page

The traversal follows the physical storage order of the FSM tree, making it I/O efficient. After processing, the function resets the next slot pointer to 0 to encourage use of low-numbered pages, which increases the likelihood that future vacuum operations can truncate the relation.

## Parameters / Member Variables
- : The relation whose Free Space Map is being vacuumed
- : FSMAddress structure identifying the specific FSM page to process
- : Starting heap block number for the range to consider
- : Ending heap block number (exclusive) for the range to consider  
- : Pointer to boolean flag set to true if the address is past the end of the FSM

## Dependencies
- Functions called/Symbols referenced:
  - fsm_readbuf
  - fsm_get_location
  - fsm_get_parent
  - fsm_get_child
  - fsm_get_avail
  - fsm_set_avail
  - fsm_get_max_avail
  - BufferGetPage
  - LockBuffer
  - MarkBufferDirtyHint
  - ReleaseBuffer
  - PageGetContents
- Called from (representative examples):
  - FreeSpaceMapVacuum
  - FreeSpaceMapVacuumRange
  - fsm_vacuum_page (recursive self-call)

## Notes and Other Information
- The function is static and only used internally within the freespace.c module
- Returns the maximum freespace value (uint8) found on the processed page
- Uses CHECK_FOR_INTERRUPTS() to allow query cancellation during long operations
- The function handles end-of-file conditions gracefully by clearing remaining slots
- Buffer locking is used only when actually updating slot values to minimize lock contention
- The next slot pointer reset is done without locking as it's considered a hint optimization