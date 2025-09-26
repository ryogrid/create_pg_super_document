# fsm_readbuf

## Location
src/backend/storage/freespace/freespace.c: 554 - 628

## Overview
Reads a Free Space Map (FSM) page from storage, handling cases where the page doesn't exist and optionally extending the FSM file when needed.

## Definition
```c
static Buffer fsm_readbuf(Relation rel, FSMAddress addr, bool extend)
```

## Detailed Description
This function provides the primary mechanism for reading FSM pages from disk storage. It handles several complex scenarios:

1. **Block Number Conversion**: Converts the logical FSM address to a physical block number using fsm_logical_to_physical()
2. **Cached Size Management**: Maintains and validates cached FSM file size information to avoid unnecessary system calls
3. **File Extension**: When `extend` is true and the requested page doesn't exist, automatically extends the FSM file
4. **Error Recovery**: Uses RBM_ZERO_ON_ERROR mode to handle corrupted pages gracefully by zeroing them out
5. **Concurrent Initialization**: Handles the complex case where multiple backends might try to initialize a new page simultaneously

The function employs a careful locking strategy for page initialization to avoid races while minimizing lock contention in the common case where pages are already initialized.

## Parameters / Member Variables
- `rel`: Relation whose FSM page is being read
- `addr`: FSMAddress specifying the logical location of the page to read
- `extend`: Boolean flag indicating whether to extend the FSM file if the page doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - fsm_logical_to_physical
  - RelationGetSmgr
  - smgrexists
  - smgrnblocks
  - fsm_extend
  - ReadBufferExtended
  - PageIsNew
  - PageInit
  - LockBuffer
  - BufferGetPage
- Called from (representative examples):
  - GetRecordedFreeSpace
  - FreeSpaceMapPrepareTruncateRel
  - fsm_set_and_search
  - fsm_search
  - fsm_vacuum_page

## Notes and Other Information
- This is a static function, only accessible within the freespace.c file
- Uses ZERO_ON_ERROR mode for reading to handle torn pages and corruption gracefully
- FSM changes are not WAL-logged, making corruption recovery important
- Implements double-checked locking pattern for page initialization to handle concurrency
- The function may return a buffer while another backend is still initializing it, which is safe for callers that will take their own buffer locks
- Cache invalidation is handled carefully to balance performance with correctness