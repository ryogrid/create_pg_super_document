# adjust_view_column_set

## Location
[src/backend/rewrite/rewriteHandler.c:3035-3108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L3035-L3108)

## Overview
Maps a set of column numbers from view columns to the corresponding columns in the underlying base relation for simply-updatable views, used primarily for column-permissions mapping.

## Definition
static Bitmapset *adjust_view_column_set(Bitmapset *cols, List *targetlist)

## Detailed Description
This function translates column references from a view context to the underlying base relation context by examining the view targetlist. It is essential for permission checking and column mapping in auto-updatable views, ensuring that column permissions and references are correctly propagated from the view to its underlying table.

The function handles both specific column references and whole-row references. For specific columns, it looks up the corresponding target list entry and extracts the underlying base relation column number. For whole-row references (represented by InvalidAttrNumber), it expands the reference to include all non-junk columns available from the view.

The mapping process assumes that relevant targetlist entries are plain Var nodes referring to columns in the underlying base relation, which should have been verified by view_query_is_auto_updatable.

## Parameters / Member Variables
- `cols`: Bitmapset containing column numbers in view context that need to be mapped to base relation context
- `targetlist`: List of TargetEntry nodes representing the view definition, used to map view columns to base relation columns

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md) (function to iterate through bitmapset members)
  - FirstLowInvalidHeapAttributeNumber (constant for attribute number offset calculation)
  - InvalidAttrNumber (constant representing whole-row references)
  - [bms_add_member](../b/bms_add_member.md) (function to add member to bitmapset)
  - [get_tle_by_resno](../g/get_tle_by_resno.md) (function to find target list entry by result number)
  - AttrNumber (type for attribute numbers)
  - [TargetEntry](../T/TargetEntry.md) (structure for target list entries)
  - Var (node type for variable references)
  - lfirst_node (macro to get list cell content with type checking)
  - castNode (macro for safe node type casting)
  - IsA (macro for type checking)
- Called from (representative examples):
  - [rewriteTargetView](../r/rewriteTargetView.md) (in src/backend/rewrite/rewriteHandler.c:3541, 3544)

## Notes and Other Information
- Column numbers in bitmapsets are offset by FirstLowInvalidHeapAttributeNumber to handle PostgreSQL internal attribute numbering
- Whole-row references to views are expanded to reference each individual column rather than being converted to whole-row references to the base relation
- This expansion preserves the view abstraction by only exposing columns that are actually available through the view
- The function skips resjunk columns during whole-row expansion since these are internal columns not visible to users
- Assumes that the view has been validated as simply-updatable and that targetlist entries for user columns are plain Var nodes
- Returns a new bitmapset containing the mapped column numbers in base relation context
- Used primarily in the query rewrite process to maintain proper column permissions and references when rewriting view operations into base table operations