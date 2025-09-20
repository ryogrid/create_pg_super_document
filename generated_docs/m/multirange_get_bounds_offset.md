# multirange_get_bounds_offset

## Location
[src/backend/utils/adt/multirangetypes.c:673-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L673-L694)

## Overview
This function calculates the byte offset of bounds values for the i-th range within a multirange by iterating through the multirange's item array and accumulating offsets until reaching the desired position.

## Definition

```c
static uint32
multirange_get_bounds_offset(const MultirangeType *multirange, int32 i)
```
## Detailed Description
The function implements an efficient offset calculation mechanism for multirange types in PostgreSQL. It works by traversing the multirange's item array backwards from the target index, accumulating the offset lengths stored in each item. The traversal stops early when it encounters an item that has an explicit offset stored (indicated by MULTIRANGE_ITEM_HAS_OFF), providing an optimization for frequently accessed ranges.

The offset calculation is crucial for the multirange data structure's space efficiency, as it allows ranges to be stored compactly without requiring explicit offset storage for every range element.

## Parameters / Member Variables
- : Pointer to the MultirangeType structure containing the multirange data
- : Zero-based index of the range for which to calculate the bounds offset

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeGetItemsPtr
  - MULTIRANGE_ITEM_GET_OFFLEN
  - MULTIRANGE_ITEM_HAS_OFF
  - MultirangeType
- Called from (representative examples):
  - [multirange_get_range](multirange_get_range.md)
  - [multirange_get_bounds](multirange_get_bounds.md)

## Notes and Other Information
- This is a static function, used internally within the multirange implementation
- The function uses an optimization where it stops traversing when it finds an item with an explicit offset
- The offset calculation works backwards from the target index for efficiency
- Returns the accumulated byte offset needed to locate the bounds of the specified range