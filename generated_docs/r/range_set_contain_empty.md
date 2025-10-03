# range_set_contain_empty

## Location
[src/backend/utils/adt/rangetypes.c:1937-1951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1937-L1951)

## Overview
Sets the RANGE_CONTAIN_EMPTY flag bit in an existing range value, used specifically for GiST index operations.

## Definition

```c
void
range_set_contain_empty(RangeType *range)
```
## Detailed Description
This function modifies an existing range object by setting the RANGE_CONTAIN_EMPTY flag bit in its flags byte. This flag is used specifically in GiST (Generalized Search Tree) operations to indicate that a range contains or represents empty ranges. The function directly modifies the flags byte at the end of the range object's binary representation. This flag is not set during normal range construction via range_serialize, but must be applied afterwards when needed for index operations.

## Parameters / Member Variables
- `*range`: Range object to modify by setting the RANGE_CONTAIN_EMPTY flag
## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE
  - RANGE_CONTAIN_EMPTY
- Called from (representative examples):
  - [range_super_union](range_super_union.md) (multiple times in GiST operations)

## Notes and Other Information
- Exclusively used for GiST index operations and not part of normal range operations
- Modifies the range object in place rather than creating a new one
- The RANGE_CONTAIN_EMPTY flag is distinct from the RANGE_EMPTY flag
- Not available through range_serialize - must be applied as a post-processing step
- Critical for proper GiST index behavior when dealing with collections that may contain empty ranges
- The flag indicates containment of empty ranges rather than the range itself being empty