# _bt_pendingfsm_add

## Location
src/backend/access/nbtree/nbtpage.c: 3062 - 3114

## Overview
_bt_pendingfsm_add maintains an array of pages deleted during the current vacuum operation, storing their metadata for later processing by _bt_pendingfsm_finalize.

## Definition
```c
static void _bt_pendingfsm_add(BTVacState *vstate, BlockNumber target, FullTransactionId safexid)
```

## Detailed Description
This static function manages the dynamic collection of deleted page metadata during a btree vacuum operation. It implements intelligent buffer management that respects work_mem limits while optimizing for the common case where deleted pages can fit within the allocated buffer.

The function maintains critical ordering guarantees: pages are always added in safexid order, which enables _bt_pendingfsm_finalize to use early termination optimization when processing the array. This ordering is enforced through assertion checking in debug builds.

When the buffer reaches capacity, the function implements a graceful degradation strategy:
1. If at maximum capacity (work_mem limit), silently discard new pages
2. If at current buffer size but below maximum, attempt to double the buffer size
3. Use repalloc() for memory-efficient buffer expansion

The function prioritizes memory efficiency by growing the buffer exponentially (doubling) up to the work_mem constraint, minimizing memory allocation overhead while respecting system resource limits.

## Parameters / Member Variables
- `vstate`: BTVacState structure containing buffer management state and the pendingpages array
- `target`: BlockNumber of the page being deleted and added to the pending list
- `safexid`: FullTransactionId representing the safe transaction ID for this deleted page (when it becomes safe to recycle)

## Dependencies
- Functions called/Symbols referenced:
  - repalloc (memory reallocation for buffer expansion)
  - FullTransactionIdFollowsOrEquals (ordering verification in debug builds)
- Structures referenced:
  - BTVacState (buffer management and page array)
  - BTPendingFSM (individual page metadata)
  - FullTransactionId (transaction ID handling)
- Called from (representative examples):
  - _bt_unlink_halfdead_page (during page deletion operations)

## Notes and Other Information
- The function is marked static, indicating it's internal to nbtpage.c and not part of the public API
- Assertion checking verifies the crucial safexid ordering invariant that _bt_pendingfsm_finalize depends on
- Buffer management implements multiple safety levels: per-call capacity check, work_mem respect, and graceful overflow handling
- Memory allocation strategy balances efficiency (exponential growth) with resource constraints (work_mem limits)
- Silent discarding of pages when at maximum capacity ensures the vacuum operation continues even under memory pressure
- The optimization automatically degrades gracefully when _bt_pendingfsm_init opted not to enable the feature (npendingpages would equal maxbufsize from the start)