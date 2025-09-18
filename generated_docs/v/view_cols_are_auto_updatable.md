# view_cols_are_auto_updatable

## Location
src/backend/rewrite/rewriteHandler.c: 2771 - 2853

## Overview
Tests whether all required columns of an auto-updatable view are actually updatable, returning NULL if all required columns can be updated or an error message for the first non-updatable required column.

## Definition
static const char *view_cols_are_auto_updatable(Query *viewquery, Bitmapset *required_cols, Bitmapset **updatable_cols, char **non_updatable_col)

## Detailed Description
This function performs detailed column-level analysis for auto-updatable views, specifically checking whether columns that need to be updated (specified in required_cols) are actually updatable. It is typically used for INSERT and UPDATE operations to ensure that the statement does not attempt to assign values to non-updatable columns.

The function can optionally return the complete set of updatable columns in the view and the name of the first non-updatable required column encountered. This information is useful for error reporting and for determining which columns can be safely included in UPDATE statements.

The caller must have already verified that the view is auto-updatable using view_query_is_auto_updatable before calling this function.

## Parameters / Member Variables
- `viewquery`: Query structure representing the view definition that has already been validated as auto-updatable
- `required_cols`: Bitmapset of column numbers that must be updatable for the current operation to succeed
- `updatable_cols`: Optional output parameter that receives a Bitmapset of all updatable column numbers in the view
- `non_updatable_col`: Optional output parameter that receives the name of the first non-updatable required column found

## Dependencies
- Functions called/Symbols referenced:
  - RangeTblRef (structure for range table references)
  - linitial_node (macro to get first list node with type checking)
  - FirstLowInvalidHeapAttributeNumber (constant for attribute numbering)
  - view_col_is_auto_updatable (function to check individual column updatability)
  - bms_add_member (function to add member to bitmapset)
  - bms_is_member (function to check bitmapset membership)
  - AttrNumber (type for attribute numbers)
  - ListCell (structure for list iteration)
  - TargetEntry (structure for target list entries)
- Called from (representative examples):
  - rewriteTargetView (in src/backend/rewrite/rewriteHandler.c:3337)

## Dependencies
- Functions called/Symbols referenced:
  - RangeTblRef (structure for range table references)
  - linitial_node (macro to get first list node with type checking)
  - FirstLowInvalidHeapAttributeNumber (constant for attribute numbering)
  - view_col_is_auto_updatable (function to check individual column updatability)
  - bms_add_member (function to add member to bitmapset)
  - bms_is_member (function to check bitmapset membership)
  - AttrNumber (type for attribute numbers)
  - ListCell (structure for list iteration)
  - TargetEntry (structure for target list entries)
- Called from (representative examples):
  - rewriteTargetView (in src/backend/rewrite/rewriteHandler.c:3337)

## Notes and Other Information
- The function assumes the view has already been validated as auto-updatable and contains exactly one base relation
- Column numbering starts from -FirstLowInvalidHeapAttributeNumber and increments for each target list entry
- Returns immediately upon finding the first non-updatable required column, making it efficient for error detection
- The updatable_cols output can be used to determine which columns are safe to include in UPDATE operations
- The non_updatable_col output provides the column name for user-friendly error messages
- Uses bitmapsets for efficient storage and manipulation of column sets
- Does not check whether the underlying base relation columns are updatable - only validates view-level constraints