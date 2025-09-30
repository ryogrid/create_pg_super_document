# fsm_extend

## Location
[src/backend/storage/freespace/freespace.c:629-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L629-L645)

## Overview
Extends the Free Space Map (FSM) fork to at least the specified number of blocks, creating new empty pages filled with zeros to indicate no free space.

## Definition
```c
static Buffer fsm_extend(Relation rel, BlockNumber fsm_nblocks)
```

## Detailed Description
This function ensures that the FSM fork has at least the requested number of blocks by extending it if necessary. The extension process creates new pages that are initialized with zeros, which in FSM terminology means they contain no information about free space (indicating the corresponding heap pages have no free space available).

The function leverages the ExtendBufferedRelTo infrastructure with specific flags:
- EB_CREATE_FORK_IF_NEEDED: Creates the FSM fork if it doesn't exist yet
- EB_CLEAR_SIZE_CACHE: Clears the cached size information to ensure accurate size tracking
- RBM_ZERO_ON_ERROR: Uses zero-on-error mode for robust error handling

The function is designed to be safe for concurrent access, as multiple backends might need to extend the FSM simultaneously during heavy insert/update workloads.

## Parameters / Member Variables
- `rel`: Relation whose FSM fork needs to be extended
- `fsm_nblocks`: Target number of blocks the FSM fork should contain (minimum size)

## Dependencies
- Functions called/Symbols referenced:
  - [ExtendBufferedRelTo](../E/ExtendBufferedRelTo.md)
  - BMR_REL (macro)
  - FSM_FORKNUM (constant)
  - EB_CREATE_FORK_IF_NEEDED (flag)
  - EB_CLEAR_SIZE_CACHE (flag)
  - RBM_ZERO_ON_ERROR (flag)
- Called from (representative examples):
  - [fsm_readbuf](fsm_readbuf.md)

## Notes and Other Information
- This is a static function, only accessible within the freespace.c file
- New FSM pages are initialized with zeros, indicating no free space information
- The function handles fork creation automatically if the FSM fork doesn't exist
- Extension operations clear the size cache to maintain consistency
- Returns a Buffer for the last block in the extended range
- Safe for concurrent execution by multiple backends
- The zero-initialization is semantically correct for FSM pages as it indicates no free space is available

## Simplified Source

```c
static Buffer
fsm_extend(Relation rel, BlockNumber fsm_nblocks)
{
    // Extend FSM fork to at least fsm_nblocks, creating empty pages as needed
    return ExtendBufferedRelTo(BMR_REL(rel), FSM_FORKNUM, NULL,
                               EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE,
                               fsm_nblocks,
                               RBM_ZERO_ON_ERROR);
}
```