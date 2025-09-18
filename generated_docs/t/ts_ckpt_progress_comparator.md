# ts_ckpt_progress_comparator

## Location
src/backend/storage/buffer/bufmgr.c: 5854 - 5876

## Overview
A comparison function designed for maintaining a min-heap data structure that tracks checkpoint completion progress across different tablespaces during buffer synchronization.

## Definition
```c
static int ts_ckpt_progress_comparator(Datum a, Datum b, void *arg)
```

## Detailed Description
The ts_ckpt_progress_comparator function provides comparison logic specifically designed for a min-heap implementation that monitors checkpoint progress across tablespaces. This comparator is used to maintain a priority queue where tablespaces with the least checkpoint progress are prioritized at the top of the heap.

The function implements inverted comparison logic typical for min-heap operations:
- Returns 1 when a < b (to prioritize lower progress values at heap top)
- Returns 0 when a == b (equal progress)  
- Returns -1 when a > b (higher progress values sink in heap)

This arrangement ensures that the tablespace with the least checkpoint progress is always readily accessible at the heap root, enabling efficient load balancing during checkpoint operations.

## Parameters / Member Variables
- `a`: Datum containing pointer to first CkptTsStatus structure
- `b`: Datum containing pointer to second CkptTsStatus structure  
- `arg`: Unused void pointer parameter (standard for heap comparator interface)

## Dependencies
- Functions called/Symbols referenced:
  - CkptTsStatus (type)
- Called from (representative examples):
  - BufferSync (checkpoint synchronization operations)
  - BufferIsPinned (heap management operations)

## Notes and Other Information
- Specifically designed for min-heap operations, hence the inverted comparison logic
- Used in checkpoint load balancing to identify tablespaces requiring more write attention
- The comparator enables efficient O(log n) heap operations for tablespace progress tracking
- Critical for maintaining balanced I/O load across multiple tablespaces during checkpoints
- Progress values represent the completion percentage or similar metric for each tablespace