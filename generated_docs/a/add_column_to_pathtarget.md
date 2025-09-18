# add_column_to_pathtarget

## Location
src/backend/optimizer/util/tlist.c: 695 - 740

## Overview
Appends a target column (expression) to an existing PathTarget structure, handling the dynamic expansion of both the expression list and sortgrouprefs array as needed.

## Definition
```c
void add_column_to_pathtarget(PathTarget *target, Expr *expr, Index sortgroupref)
```

## Detailed Description
This function extends a PathTarget by adding a new expression to its expression list and managing the corresponding sortgroupref information. The function handles three scenarios for sortgrouprefs: (1) if the target already has sortgrouprefs, it extends the array using repalloc and sets the new entry; (2) if the target has no sortgrouprefs but the new expression needs one, it allocates a new array with palloc0 and sets the appropriate entry; (3) if neither the target nor the new expression uses sortgrouprefs, no additional array management is needed.

The function also resets the volatility status to unknown when adding a new expression, allowing contain_volatile_functions to properly re-evaluate the volatility of the entire PathTarget. Like make_pathtarget_from_tlist, this function leaves cost and width calculations to the caller.

## Parameters / Member Variables
- `target`: The PathTarget structure to which the column should be added
- `expr`: The expression node representing the new column
- `sortgroupref`: The sort group reference index for the new column (0 if not applicable)

## Dependencies
- Functions called/Symbols referenced:
  - PathTarget (data structure)
  - lappend (list append)
  - list_length (list utility)
  - repalloc (memory reallocation)
  - palloc0 (zero-initialized memory allocation)
  - VOLATILITY_NOVOLATILE (volatility constant)
  - VOLATILITY_UNKNOWN (volatility constant)
- Called from (representative examples):
  - create_one_window_path
  - make_group_input_target
  - make_partial_grouping_target
  - make_window_input_target
  - make_sort_input_target
  - add_new_column_to_pathtarget
  - add_sp_item_to_pathtarget

## Notes and Other Information
- Dynamically manages sortgrouprefs array expansion as needed
- Resets volatility status to unknown when new expressions are added
- Cost and width fields are left for the caller to update
- Handles the transition from no sortgrouprefs to having sortgrouprefs
- Uses repalloc for efficiency when extending existing sortgrouprefs array
- The function is declared in src/include/optimizer/tlist.h