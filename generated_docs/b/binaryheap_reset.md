# binaryheap_reset

## Location
[src/common/binaryheap.c:63-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L63-L74)

## Overview
Resets an existing binary heap to an empty state while preserving its capacity and configuration parameters.

## Definition
```c
void binaryheap_reset(binaryheap *heap)
```

## Detailed Description
The `binaryheap_reset` function efficiently clears all data from a binary heap without deallocating memory or changing the heap's configuration. It sets the heap size to zero and restores the heap property flag to true, effectively returning the heap to its initial empty state. The heap's capacity, comparison function, and user argument remain unchanged, allowing the heap to be reused without reallocation.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [binaryheap](binaryheap.md) (struct type)
- Called from (representative examples):
  - [gather_merge_init](../g/gather_merge_init.md)
  - [ExecReScanMergeAppend](../E/ExecReScanMergeAppend.md)
  - [pgarch_readyXlog](../p/pgarch_readyXlog.md)

## Notes and Other Information
- This is a very efficient operation as it only modifies two integer fields
- No memory deallocation occurs, making this suitable for heap reuse scenarios
- The heap's original capacity and comparison function are preserved
- After reset, the heap can immediately accept new elements through normal insertion operations
- Commonly used in scenarios where the same heap is reused across multiple operations or iterations

## Simplified Source

```c
void binaryheap_reset(binaryheap *heap) {
    // Clear all elements from the heap
    heap->bh_size = 0;

    // Mark heap as having proper heap property (empty heap always satisfies this)
    heap->bh_has_heap_property = true;
}
```