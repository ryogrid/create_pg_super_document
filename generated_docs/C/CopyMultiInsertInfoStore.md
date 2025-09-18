# CopyMultiInsertInfoStore

## Location
src/backend/commands/copyfrom.c: 605 - 627

## Overview
Records a previously reserved TupleTableSlot as consumed and updates buffering statistics during COPY FROM operations.

## Definition
```c
static inline void
CopyMultiInsertInfoStore(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
                        TupleTableSlot *slot, int tuplen, uint64 lineno)
```

## Detailed Description
This function completes the process of storing a tuple in the multi-insert buffer by marking a previously reserved slot (obtained via CopyMultiInsertInfoNextFreeSlot) as consumed. It updates both the buffer's usage tracking and the overall multi-insert statistics. The function records the source line number for error reporting purposes and tracks the cumulative size of buffered data for memory management decisions.

This function works in conjunction with CopyMultiInsertInfoNextFreeSlot to manage the lifecycle of buffered tuples - first a slot is reserved, then the tuple is stored in it, and finally this function marks it as consumed.

## Parameters / Member Variables
- `miinfo`: CopyMultiInsertInfo pointer for tracking overall buffer statistics
- `rri`: ResultRelInfo pointer containing the target relation and its buffer
- `slot`: TupleTableSlot that was previously reserved and now contains the tuple
- `tuplen`: Size in bytes of the stored tuple for memory tracking
- `lineno`: Source line number for error reporting if insertion fails later

## Dependencies
- Functions called/Symbols referenced:
  - CopyMultiInsertInfo (struct)
  - CopyMultiInsertBuffer (struct)
- Called from (representative examples):
  - CopyFrom (at line 1220)

## Notes and Other Information
- This is a static inline function for performance optimization
- Must be called with a slot that was previously obtained from CopyMultiInsertInfoNextFreeSlot
- The function asserts that the provided slot matches the expected next slot in the buffer
- Line numbers are stored for accurate error reporting during batch insert operations
- Updates both per-buffer (nused) and global (bufferedTuples, bufferedBytes) statistics
- Part of the multi-insert buffering mechanism that improves COPY performance by batching operations