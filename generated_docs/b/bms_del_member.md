# bms_del_member

## Location
[src/backend/nodes/bitmapset.c:868-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L868-L916)

## Overview
Removes a specified member from a bitmapset, automatically trimming trailing empty words and freeing the set if it becomes empty.

## Definition

```c
Bitmapset *
bms_del_member(Bitmapset *a, int x)
```
## Detailed Description
The  function removes the specified integer member  from bitmapset . If the member is not present in the set, the function returns the set unchanged without error. The function performs automatic cleanup by trimming trailing empty words when the last word becomes empty after deletion, and completely frees the bitmapset if it becomes empty, returning NULL.

The function uses bitwise AND with a complement mask to clear the specific bit, then checks if optimization is needed. When the deleted bit was in the last word and that word becomes zero, it scans backwards to find the last non-empty word and adjusts the word count accordingly.

## Parameters / Member Variables
- : Input Bitmapset to modify (can be NULL, which returns NULL unchanged)
- : Integer member to remove from the set (must be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  - : Validates the input bitmapset structure
  - : Macro to calculate which bitmap word contains the member
  - : Macro to calculate bit position within the word
  - : Optional function for memory management (when REALLOCATE_BITMAPSETS is defined)
  - : Type for individual bitmap storage words
  - : PostgreSQL memory deallocation function
- Called from (representative examples):
  - : Hash column selection in aggregation
  - : Index path building optimization
  - : Relation removal during join elimination
  - : Restrictinfo relation removal
  - : Outer join information processing
  - : Join domain minimum relation calculation

## Notes and Other Information
- Does not error if the member to remove is not present in the set
- Returns an error for negative member values using 
- Returns NULL for NULL input without error
- Automatically trims trailing empty words to maintain compact representation
- Returns NULL and frees memory when the set becomes completely empty
- Under  compile flag, performs copy-and-free before modification
- Uses  hint for the case where member is beyond current word range
- Essential for query optimization operations that need to remove relations or attributes from consideration
- Maintains memory efficiency by not keeping unnecessary trailing zero words