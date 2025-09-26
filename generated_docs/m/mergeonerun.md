# mergeonerun

## Location
[src/backend/utils/sort/tuplesort.c:2232-2291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2232-L2291)

## Overview
Merges one run from each active input tape by maintaining a min-heap to select the smallest tuple across all input sources and writing it to the output tape.

## Definition

```c
static void
mergeonerun(Tuplesortstate *state)
```
## Detailed Description
The  function implements the core merge logic for external sorting by performing a k-way merge of one run from each input tape. It uses a min-heap data structure to efficiently determine the next smallest tuple across all input sources.

**Algorithm Steps:**
1. **Initialization**: Calls  to load one tuple from each input tape into the heap
2. **Main Merge Loop**: Repeatedly:
   - Extracts the smallest tuple (heap root) 
   - Writes it to the destination tape via 
   - Releases the slab slot used by the written tuple
   - Reads the next tuple from the same source tape
   - Either replaces the heap top with the new tuple or removes the exhausted tape from the heap
3. **Completion**: Writes an end-of-run marker using  when all input runs are consumed

The function efficiently handles tape exhaustion by removing empty tapes from the heap and decrementing the input run counter. The heap maintains the merge invariant that the root always contains the globally smallest unprocessed tuple.

## Parameters / Member Variables
- : Pointer to the  structure containing:
  - : Heap array storing one tuple from each active input tape
  - : Number of active tuples in the heap
  - : Array of input tape objects
  - : Current output tape for writing merged results
  - : Number of remaining input runs to process
  - : Must be true for tuple memory management

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes the heap with one tuple from each input tape
  - : Macro to write a tuple to the destination tape
  - : Macro to free slab-allocated tuple memory
  - : Reads the next tuple from a specific input tape
  - : Replaces heap root with new tuple and reheapifies
  - : Removes heap root and reheapifies
  - : Writes end-of-run marker to tape
  - : Tuple structure type
  - : Logical tape structure type

- Called from (representative examples):
  - : During each merge pass to process runs from input tapes

## Notes and Other Information
- This is a static function within tuplesort.c, internal to the sorting implementation  
- Implements the k-way merge component of external merge sort algorithm
- Uses heap data structure for O(log k) tuple selection where k is the number of input tapes
- Requires slab allocator to be active for efficient tuple memory management
- The function processes exactly one run from each input tape per call
- Handles variable-length runs gracefully by removing exhausted tapes from the merge
- Critical for maintaining sorted order during multi-pass external sorting
- Memory management uses slab allocation for consistent performance with many small allocations
- Works in conjunction with  (from the related processed symbols) to properly delimit runs on output tapes