# find_any_idle_slot

## Location
[src/fe_utils/parallel_slot.c:179-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/parallel_slot.c#L179-L195)

## Overview
find_any_idle_slot is a static utility function that searches through a parallel slot array to find the first available (idle) slot for use in parallel operations.

## Definition

```c
struct the fd_set for each call to select_loop */
	FD_ZERO(&slotset);
```
## Detailed Description
This function performs a linear search through the slots in a ParallelSlotArray to locate the first slot that is not currently in use. It iterates through all slots sequentially, checking the inUse flag of each slot. The function returns immediately upon finding the first idle slot, making it efficient for cases where idle slots are commonly available early in the array.

## Parameters / Member Variables
- : A const pointer to the ParallelSlotArray structure containing the slots to search through

## Dependencies
- Functions called/Symbols referenced:
  - [ParallelSlotArray](../P/ParallelSlotArray.md) (structure type)
- Called from (representative examples):
  - [ParallelSlotsGetIdle](../P/ParallelSlotsGetIdle.md)

## Notes and Other Information
- Returns the zero-based index of the first idle slot, or -1 if all slots are busy
- Uses a simple linear search algorithm with O(n) time complexity
- The function is static, meaning it's only accessible within the parallel_slot.c compilation unit
- The search is performed from index 0 to numslots-1, so lower-indexed slots are preferred for allocation

## Simplified Source

```c
static int find_any_idle_slot(const ParallelSlotArray *sa) {
    // Linear search through all slots to find first idle one
    for (int i = 0; i < sa->numslots; i++) {
        if (!sa->slots[i].inUse) {
            return i;  // Return index of first idle slot
        }
    }
    return -1;  // All slots are busy
}
```