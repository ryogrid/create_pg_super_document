# ParallelBitmapHeapState

## Location
[src/include/nodes/execnodes.h:1784-1793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L1784-L1793)

## Overview
ParallelBitmapHeapState is a shared state structure that coordinates parallel bitmap heap scan operations across multiple worker processes, managing synchronized access to TID bitmap iterators and prefetching coordination.

## Definition

```c
typedef struct ParallelBitmapHeapState
{
	dsa_pointer tbmiterator;
	dsa_pointer prefetch_iterator;
	slock_t		mutex;
	int			prefetch_pages;
	int			prefetch_target;
	SharedBitmapState state;
	ConditionVariable cv;
} ParallelBitmapHeapState;
```
## Detailed Description
ParallelBitmapHeapState manages the coordination of parallel bitmap heap scan operations where multiple worker processes collaborate to scan heap pages identified by a shared TID bitmap. The structure maintains shared iterators for both current scanning and prefetch operations, ensuring that worker processes don't duplicate work while maximizing I/O efficiency through coordinated prefetching. It uses dynamic shared area (DSA) pointers to reference shared bitmap iterators, mutual exclusion mechanisms to coordinate access, and condition variables for worker synchronization.

## Parameters / Member Variables
- `tbmiterator`: DSA pointer to the shared TID bitmap iterator for scanning current pages
- `prefetch_iterator`: DSA pointer to the shared TID bitmap iterator for prefetching ahead of current page
- `mutex`: Spinlock for mutual exclusion when accessing prefetching variables and shared state
- `prefetch_pages`: Number of pages the prefetch iterator is ahead of the current iterator
- `prefetch_target`: Current target prefetch distance for optimal I/O performance
- `state`: SharedBitmapState indicating the current state of the TID bitmap (e.g., building, ready, done)
- `cv`: Condition variable for coordinating worker processes during state transitions
## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer
  - [slock_t](../s/slock_t.md)
  - SharedBitmapState
  - ConditionVariable
- Called from (representative examples):
  - [BitmapHeapNext](../B/BitmapHeapNext.md)
  - [ExecBitmapHeapInitializeDSM](../E/ExecBitmapHeapInitializeDSM.md)
  - [ExecBitmapHeapInitializeWorker](../E/ExecBitmapHeapInitializeWorker.md)
  - [BitmapDoneInitializingSharedState](../B/BitmapDoneInitializingSharedState.md)
  - [BitmapAdjustPrefetchIterator](../B/BitmapAdjustPrefetchIterator.md)
  - [BitmapPrefetch](../B/BitmapPrefetch.md)

## Notes and Other Information
- Essential for parallel bitmap heap scans where multiple workers collaborate on scanning heap pages
- The prefetch mechanism optimizes I/O by reading ahead of the current scan position to reduce seek time
- Dynamic shared area (DSA) pointers allow the iterators to be shared across process boundaries
- The mutex protects critical sections where multiple workers might modify shared prefetch state
- Condition variables enable efficient waiting when workers need to synchronize on bitmap state changes
- The prefetch target is dynamically adjusted based on I/O patterns and system performance
- This structure is allocated in shared memory and accessible to all participating worker processes
- Used in conjunction with BitmapHeapScanState for the complete parallel bitmap scan implementation