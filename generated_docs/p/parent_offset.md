# parent_offset

## Location
[src/common/binaryheap.c:102-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L102-L115)

## Overview
A static inline utility function that calculates the parent node index in a binary heap data structure using 0-based array indexing.

## Definition

```c
static inline int
parent_offset(int i)
```
## Detailed Description
This function implements the standard binary heap parent index calculation formula for 0-based array indexing. In a binary heap stored as an array, for any node at index i (where i > 0), its parent is located at index (i-1)/2. This function is essential for heap operations that need to traverse upward in the heap tree structure, such as maintaining heap properties during insertions or heap construction.

## Parameters / Member Variables
- : The index of a node in the binary heap array for which to find the parent index

## Dependencies
- Functions called/Symbols referenced: None (basic arithmetic operation)
- Called from (representative examples):
  - ltsReleaseBlock (in src/backend/utils/sort/logtape.c)
  - [binaryheap_build](../b/binaryheap_build.md) (in src/common/binaryheap.c)  
  - [sift_up](../s/sift_up.md) (in src/common/binaryheap.c)

## Notes and Other Information
- This is a static inline function for performance optimization since it's a simple calculation used frequently in heap operations
- The function assumes 0-based indexing where the root node is at index 0
- For the root node (index 0), this function would return 0 due to integer division, but callers should not invoke this function with i=0 as the root has no parent
- Used in binary heap implementations throughout PostgreSQL for efficient priority queue operations and heap maintenance