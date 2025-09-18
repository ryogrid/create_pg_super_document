# list_append_unique_ptr

## Location
src/backend/nodes/list.c: 1356 - 1367

## Overview
Appends a datum to a list only if it is not already present in the list, using simple pointer equality to determine membership.

## Definition
```c
List *list_append_unique_ptr(List *list, void *datum)
```

## Detailed Description
This function is a specialized variant of `list_append_unique()` that provides a convenient way to append a pointer element to a list while ensuring no duplicates are created. Before appending the datum, it first checks whether the exact same pointer is already present in the list using simple pointer equality (==) comparison rather than deep content comparison.

If the pointer is found to be a member (same memory address), the original list is returned unchanged. If the pointer is not found, it is appended to the list using `lappend()`. This variant is more efficient than the regular `list_append_unique()` when working with pointer lists where identity matters more than content equality.

## Parameters / Member Variables
- `list`: The target list to append to (can be NIL)
- `datum`: The pointer element to append if not already present

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_ptr](list_member_ptr.md) - Checks if datum pointer is already in the list using pointer equality
  - `lappend` - Appends the datum to the list if not already present
- Called from (representative examples):
  - `subbuild_joinrel_restrictlist` (src/backend/optimizer/util/relnode.c:1403)
  - `subbuild_joinrel_joinlist` (src/backend/optimizer/util/relnode.c:1448)

## Notes and Other Information
- Uses simple pointer equality (==) for membership testing, not content comparison
- More efficient than `list_append_unique()` for pointer lists since it avoids deep comparison
- Performs a linear search, but faster than content-based comparison
- Returns the original list pointer if datum is already present
- Returns a new list pointer if datum was appended
- The list can be NIL (empty) - in this case, a new single-element list is created
- Suitable for maintaining lists of unique pointers where identity matters
- Commonly used in optimizer code for managing lists of relation pointers and similar objects