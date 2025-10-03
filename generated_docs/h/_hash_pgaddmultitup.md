# _hash_pgaddmultitup

## Location
[src/backend/access/hash/hashinsert.c:331-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashinsert.c#L331-L369)

## Overview
The  function adds multiple index tuples to a specific page in a hash index while maintaining hashkey ordering, used primarily during bulk operations like bucket splits and page reorganization.

## Definition

```c
void
_hash_pgaddmultitup(Relation rel, Buffer buf, IndexTuple *itups,
					OffsetNumber *itup_offsets, uint16 nitups)
```
## Detailed Description
This function performs bulk insertion of multiple tuples into a hash index page. Unlike  which handles single tuple insertion, this function is optimized for scenarios where multiple tuples need to be inserted into the same page, such as during:

- Bucket splitting operations where tuples are redistributed
- Page reorganization during overflow page management
- Bulk tuple movement during vacuum operations

For each tuple in the input array, the function:
1. Calculates the tuple size with proper alignment
2. Determines the correct insertion position using binary search to maintain hashkey ordering
3. Inserts the tuple at the calculated position
4. Records the offset number where each tuple was inserted

The function maintains the same locking and ordering requirements as  but processes multiple tuples in a single operation for efficiency.

## Parameters / Member Variables
- `rel`: The hash index relation
- `buf`: Buffer containing the target page (must be pinned and write-locked)
- `*itups`: Array of index tuples to be inserted
- `*itup_offsets`: Output array to store the offset numbers where each tuple was inserted
- `nitups`: Number of tuples in the input array
## Dependencies
- Functions called/Symbols referenced:
  - : Validate page type (bucket or overflow page)
  - : Calculate size of each tuple
  - : Extract hashkey from each tuple
  - : Binary search for correct insertion position
  - : Add each tuple to the page
  - : Get relation name for error reporting

- Called from (representative examples):
  - : During overflow page deallocation
  - : During bucket consolidation
  - : During bucket splitting operations

## Notes and Other Information
- The caller must hold both a pin and write lock on the target buffer before calling this function
- The function does not write the page to disk; that responsibility lies with the caller
- Unlike , this function does not support an append mode - all insertions use binary search for proper positioning
- The function processes tuples sequentially, maintaining page ordering after each insertion
- Each tuple's size is aligned using MAXALIGN for proper memory alignment
- Returns void but populates the  array with the insertion positions
- Used primarily for internal bulk operations rather than user-initiated insertions
- More efficient than multiple calls to  when inserting multiple tuples to the same page