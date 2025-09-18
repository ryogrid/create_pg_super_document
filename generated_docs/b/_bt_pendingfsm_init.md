# _bt_pendingfsm_init

## Location
src/backend/access/nbtree/nbtpage.c: 2954 - 2994

## Overview
_bt_pendingfsm_init initializes local memory state used by VACUUM for the _bt_pendingfsm_finalize optimization, preparing a buffer to track pages deleted during a btree vacuum operation.

## Definition
```c
void _bt_pendingfsm_init(Relation rel, BTVacState *vstate, bool cleanuponly)
```

## Detailed Description
This function sets up memory management for tracking newly deleted pages during a btree vacuum operation. The optimization aims to efficiently place deleted pages into the free space map by deferring FSM updates until the end of the vacuum process when it's safe to recycle pages.

The function allocates a dynamic buffer within VACUUM's top-level memory context, with size constraints based on the work_mem setting. When the buffer reaches capacity, the optimization gracefully degrades by stopping collection of additional deleted pages while continuing to process pages that fit within memory limits.

The function implements a key optimization principle: rather than immediately placing deleted pages in the FSM (which might be unsafe due to concurrent transactions), it defers FSM updates until _bt_pendingfsm_finalize can safely determine which pages are truly recyclable.

## Parameters / Member Variables
- `rel`: Relation being vacuumed (B-tree index)
- `vstate`: BTVacState structure containing vacuum state information that will be populated with buffer management fields
- `cleanuponly`: Boolean indicating if this is a cleanup-only vacuum operation (no new page deletions expected)

## Dependencies
- Functions called/Symbols referenced:
  - work_mem (global variable for memory limit)
  - [palloc](../p/palloc.md) (memory allocation)
  - Min/Max macros
  - MaxAllocSize constant
  - [BTPendingFSM](../B/BTPendingFSM.md) structure
  - [BTVacState](../B/BTVacState.md) structure fields (bufsize, maxbufsize, pendingpages, npendingpages)
- Called from (representative examples):
  - [btvacuumscan](btvacuumscan.md)

## Notes and Other Information
- The function uses a conservative approach with cleanup-only operations, completely skipping the optimization since no new deletions are expected
- Initial buffer size is set to 256 entries, with dynamic growth capability up to work_mem limits
- Memory size calculations carefully avoid integer overflow by using appropriate type conversions and bounds checking
- Buffer size management implements multiple safety checks: work_mem limit, integer overflow protection (INT_MAX), and allocator limits (MaxAllocSize)
- The optimization may not be effective in all scenarios - if it fails, subsequent vacuum operations may need to fall back to cleanup-only mode