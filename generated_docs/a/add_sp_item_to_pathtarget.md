# add_sp_item_to_pathtarget

## Location
[src/backend/optimizer/util/tlist.c:1202-1247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L1202-L1247)

## Overview
Adds a split_pathtarget_item to a PathTarget, unless a matching item is already present, with intelligent handling of sortgrouprefs during the process.

## Definition


## Detailed Description
This function is a specialized version of add_new_column_to_pathtarget designed to work with split_pathtarget_item structures. It intelligently merges items based on expression equality while handling sortgrouprefs appropriately. The function implements a merging strategy where an item with zero sortgroupref can be merged with one that has a sortgroupref, acquiring the latter's sortgroupref value.

The function first searches for a pre-existing entry in the PathTarget that matches the item's expression using equal() and has a compatible sortgroupref. If such a match is found, the function updates the sortgroupref if needed and returns without adding a duplicate. If no match is found, it adds the item to the PathTarget as a new column.

The design assumes that the target PathTarget does not already contain duplicate sortgrouprefs, which should be guaranteed if the original target passed to split_pathtarget_at_srfs was properly formed.

## Parameters / Member Variables
- : The PathTarget to which the item should be added
- : The split_pathtarget_item containing the expression and sortgroupref to be added

## Dependencies
- Functions called/Symbols referenced:
  - get_pathtarget_sortgroupref
  - [equal](../e/equal.md)
  - [add_column_to_pathtarget](add_column_to_pathtarget.md)
  - copyObject
  - [palloc0](../p/palloc0.md)
  - list_length
- Called from (representative examples):
  - [add_sp_items_to_pathtarget](add_sp_items_to_pathtarget.md)
  - [split_pathtarget_at_srfs](../s/split_pathtarget_at_srfs.md)

## Notes and Other Information
- This is a static function internal to tlist.c, specifically designed for use in split pathtarget operations
- The function assumes that duplicate sortgrouprefs in the target are impossible unless the original target already had duplicates
- Memory safety is ensured by copying the expression using copyObject before adding it to the PathTarget
- The sortgrouprefs array is allocated on-demand when needed, initialized with palloc0 for proper zero-initialization