# searchChar

## Location
[src/test/modules/spgist_name_ops/spgist_name_ops.c:97-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/spgist_name_ops/spgist_name_ops.c#L97-L123)

## Overview
A binary search function that searches an array of int16 datums for a specific character value, returning both the search result and the insertion position.

## Definition

```c
static bool
searchChar(Datum *nodeLabels, int nNodes, int16 c, int *i)
```
## Detailed Description
This function performs a binary search on a sorted array of PostgreSQL Datum values containing int16 characters. It efficiently locates a target character or determines where it should be inserted to maintain sorted order. The function is optimized for SP-GiST operations where node labels are stored as sorted arrays of characters, enabling fast traversal decisions during index operations.

The binary search algorithm divides the search space in half at each step, providing O(log n) time complexity. When a match is found, the function returns true and sets the index. When no match is found, it returns false but still provides the insertion point where the character would maintain sorted order.

## Parameters / Member Variables
- `nodeLabels`: Array of Datum values containing int16 characters to search
- `nNodes`: Number of elements in the nodeLabels array
- `c`: Target int16 character value to search for
- `i`: Pointer to integer that receives the result index (match position or insertion point)

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInt16 (extracts int16 value from Datum)
- Called from (representative examples):
  - [spg_text_choose](spg_text_choose.md)
  - [spgist_name_choose](spgist_name_choose.md)

## Notes and Other Information
- Located in src/backend/access/spgist/spgtextproc.c:158-183
- Static function, only accessible within the same compilation unit
- Time complexity: O(log n) for search operation
- Assumes input array is sorted in ascending order
- Sets *i to insertion point even on failed searches, making it useful for both lookup and insertion operations
- Uses bit shifting (>> 1) for efficient division by 2 in middle calculation
- Essential for SP-GiST node traversal where character labels guide the search path