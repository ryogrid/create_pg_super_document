# list_append_unique_ptr

## Location
[src/backend/nodes/list.c:1356-1367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1356-L1367)

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
  - `[lappend](lappend.md)` - Appends the datum to the list if not already present
- Called from (representative examples):
  - `[subbuild_joinrel_restrictlist](../s/subbuild_joinrel_restrictlist.md)` (src/backend/optimizer/util/relnode.c:1403)
  - `[subbuild_joinrel_joinlist](../s/subbuild_joinrel_joinlist.md)` (src/backend/optimizer/util/relnode.c:1448)

## Notes and Other Information
- Uses simple pointer equality (==) for membership testing, not content comparison
- More efficient than `list_append_unique()` for pointer lists since it avoids deep comparison
- Performs a linear search, but faster than content-based comparison
- Returns the original list pointer if datum is already present
- Returns a new list pointer if datum was appended
- The list can be NIL (empty) - in this case, a new single-element list is created
- Suitable for maintaining lists of unique pointers where identity matters
- Commonly used in optimizer code for managing lists of relation pointers and similar objects

## Simplified Source

```c
List *list_append_unique_ptr(List *list, void *datum) {
    // Check if pointer already exists in list
    if (list_member_ptr(list, datum))
        return list;  // Already present, return unchanged

    // Not found, append to list
    return lappend(list, datum);
}
```

This simplified version shows the core logic: check for pointer existence using `list_member_ptr()`, and if not found, append using `lappend()`. The function ensures unique pointers in the list using simple pointer equality comparison.