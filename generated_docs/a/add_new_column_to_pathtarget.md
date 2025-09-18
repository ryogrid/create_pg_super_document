# add_new_column_to_pathtarget

## Location
src/backend/optimizer/util/tlist.c: 741 - 751

## Overview
Appends a target column to a PathTarget, but only if it's not equal() to any pre-existing target expression in the PathTarget.

## Definition


## Detailed Description
This function provides a safe way to add a new expression column to a PathTarget structure while avoiding duplicates. It first checks if the given expression already exists in the target's expression list using list_member(), and only adds it if it's not found. The function delegates the actual addition to add_column_to_pathtarget() with a sortgroupref of 0, since the caller cannot specify a sortgroupref when using this function (it would be unclear how to merge that with a pre-existing column).

The function leaves it to the caller to update the cost and width fields of the PathTarget after the addition, similar to the behavior of make_pathtarget_from_tlist.

## Parameters / Member Variables
- : The PathTarget structure to which the expression should be added
- : The expression to add to the PathTarget's expression list

## Dependencies
- Functions called/Symbols referenced:
  - [list_member](../l/list_member.md) (checks if expression already exists in target->exprs)
  - [add_column_to_pathtarget](add_column_to_pathtarget.md) (performs the actual addition with sortgroupref=0)
  - [PathTarget](../P/PathTarget.md) (the target structure type)
- Called from (representative examples):
  - [add_new_columns_to_pathtarget](add_new_columns_to_pathtarget.md) (in src/backend/optimizer/util/tlist.c:760)

## Notes and Other Information
- The caller cannot specify a sortgroupref when using this function, as it would be unclear how to merge that with a pre-existing column
- Cost and width fields of the PathTarget are not automatically updated and must be handled by the caller
- This is a utility function that prevents duplicate expressions in PathTargets during query planning