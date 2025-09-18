# count_nonjunk_tlist_entries

## Location
src/backend/optimizer/util/tlist.c: 186 - 217

## Overview
Counts the number of non-resjunk entries in a target list, returning the count of user-visible columns.

## Definition


## Detailed Description
The `count_nonjunk_tlist_entries` function iterates through a target list and counts only the TargetEntry nodes that are not marked as resjunk. Resjunk columns are auxiliary columns used internally by PostgreSQL for processing but are not part of the final result set visible to users. This function provides a quick way to determine how many actual output columns a target list will produce.

## Parameters / Member Variables
- `tlist`: The target list to count non-resjunk entries from

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic list iteration)
- Called from (representative examples):
  - transformMultiAssignRef
  - transformSubLink
  - transformJsonArrayQueryConstructor
  - get_update_query_targetlist_def

## Notes and Other Information
- Returns an integer count of non-resjunk entries
- Simple utility function with straightforward linear iteration
- Critical for determining the actual width of result tuples
- Used in parser transformations and rule utilities
- Helps distinguish between internal processing columns and user-visible output columns
- Essential for proper query result formatting and tuple structure determination