# add_column_to_pathtarget

## Location
[src/backend/optimizer/util/tlist.c:695-740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L695-L740)

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
  - [PathTarget](../P/PathTarget.md) (data structure)
  - [lappend](../l/lappend.md) (list append)
  - [list_length](../l/list_length.md) (list utility)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - VOLATILITY_NOVOLATILE (volatility constant)
  - VOLATILITY_UNKNOWN (volatility constant)
- Called from (representative examples):
  - [create_one_window_path](../c/create_one_window_path.md)
  - [make_group_input_target](../m/make_group_input_target.md)
  - [make_partial_grouping_target](../m/make_partial_grouping_target.md)
  - [make_window_input_target](../m/make_window_input_target.md)
  - [make_sort_input_target](../m/make_sort_input_target.md)
  - [add_new_column_to_pathtarget](add_new_column_to_pathtarget.md)
  - [add_sp_item_to_pathtarget](add_sp_item_to_pathtarget.md)

## Notes and Other Information
- Dynamically manages sortgrouprefs array expansion as needed
- Resets volatility status to unknown when new expressions are added
- Cost and width fields are left for the caller to update
- Handles the transition from no sortgrouprefs to having sortgrouprefs
- Uses repalloc for efficiency when extending existing sortgrouprefs array
- The function is declared in src/include/optimizer/tlist.h

## Simplified Source

```c
void
add_column_to_pathtarget(PathTarget *target, Expr *expr, Index sortgroupref)
{
    // Add expression to the target list
    target->exprs = lappend(target->exprs, expr);

    // Handle sortgrouprefs array
    if (target->sortgrouprefs) {
        // Extend existing sortgrouprefs array
        int nexprs = list_length(target->exprs);
        target->sortgrouprefs = (Index *)
            repalloc(target->sortgrouprefs, nexprs * sizeof(Index));
        target->sortgrouprefs[nexprs - 1] = sortgroupref;
    } else if (sortgroupref) {
        // Create new sortgrouprefs array (previously unlabeled target)
        int nexprs = list_length(target->exprs);
        target->sortgrouprefs = (Index *) palloc0(nexprs * sizeof(Index));
        target->sortgrouprefs[nexprs - 1] = sortgroupref;
    }

    // Reset volatility to unknown - let contain_volatile_functions re-evaluate
    if (target->has_volatile_expr == VOLATILITY_NOVOLATILE)
        target->has_volatile_expr = VOLATILITY_UNKNOWN;
}
```