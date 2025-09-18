# add_new_columns_to_pathtarget

## Location
src/backend/optimizer/util/tlist.c: 752 - 773

## Overview
Applies add_new_column_to_pathtarget() for each element in a list of expressions, efficiently adding multiple columns to a PathTarget while avoiding duplicates.

## Definition
void add_new_columns_to_pathtarget(PathTarget *target, List *exprs)

## Detailed Description
This function serves as a batch operation wrapper around add_new_column_to_pathtarget(). It iterates through a list of expressions and applies add_new_column_to_pathtarget() to each one, which ensures that only unique expressions are added to the PathTarget. This is commonly used during query planning when multiple expressions need to be added to a PathTarget structure, such as when creating input targets for grouping, windowing, or sorting operations.

The function uses PostgreSQL's foreach macro to iterate through the list efficiently, casting each list element to an Expr pointer before passing it to add_new_column_to_pathtarget().

## Parameters / Member Variables
- target: The PathTarget structure to which the expressions should be added
- exprs: A List of Expr nodes to be added to the PathTarget

## Dependencies
- Functions called/Symbols referenced:
  - add_new_column_to_pathtarget (adds individual expressions while avoiding duplicates)
  - PathTarget (the target structure type)
  - List, ListCell, foreach, lfirst (PostgreSQL list manipulation utilities)
- Called from (representative examples):
  - make_group_input_target (in src/backend/optimizer/plan/planner.c:5582)
  - make_partial_grouping_target (in src/backend/optimizer/plan/planner.c:5668)
  - make_window_input_target (in src/backend/optimizer/plan/planner.c:6178)
  - make_sort_input_target (in src/backend/optimizer/plan/planner.c:6479)

## Notes and Other Information
- This is a convenience function that provides batch processing of expressions for PathTarget construction
- Like add_new_column_to_pathtarget(), it leaves cost and width field updates to the caller
- Commonly used in query planning phases where multiple expressions need to be consolidated into PathTargets
- The function inherits the duplicate-avoidance behavior from add_new_column_to_pathtarget()