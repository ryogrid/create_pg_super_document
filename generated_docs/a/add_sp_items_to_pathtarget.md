# add_sp_items_to_pathtarget

## Location
[src/backend/optimizer/util/tlist.c:1248-1258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L1248-L1258)

## Overview
Applies add_sp_item_to_pathtarget to each element of a list, efficiently adding multiple split_pathtarget_items to a PathTarget.

## Definition
```c
static void add_sp_items_to_pathtarget(PathTarget *target, List *items)
```

## Detailed Description
This function serves as a convenience wrapper that iterates through a list of split_pathtarget_item structures and adds each one to the specified PathTarget using add_sp_item_to_pathtarget. It provides a simple way to batch-process multiple items while leveraging the intelligent merging and sortgroupref handling provided by the underlying add_sp_item_to_pathtarget function.

The function maintains the same semantics as its underlying function: duplicate expressions are avoided, and sortgrouprefs are handled appropriately during the merge process.

## Parameters / Member Variables
- `target`: The PathTarget to which all items in the list should be added
- `items`: A List containing split_pathtarget_item structures to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [add_sp_item_to_pathtarget](add_sp_item_to_pathtarget.md)
  - lfirst (via foreach macro)
- Called from (representative examples):
  - [split_pathtarget_at_srfs](../s/split_pathtarget_at_srfs.md)

## Notes and Other Information
- This is a static function internal to tlist.c, designed for batch operations during split pathtarget processing
- The function assumes that the items list contains only split_pathtarget_item pointers
- All the safety guarantees and behavior of add_sp_item_to_pathtarget apply to each item processed by this function
- The function processes items in the order they appear in the list