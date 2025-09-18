# _bt_binsrch

## Location
src/backend/access/nbtree/nbtsearch.c: 337 - 467

## Overview
This function performs binary search on a B-tree page to find the appropriate position for a given scan key, handling both internal and leaf pages with different search semantics.

## Definition


## Detailed Description
_bt_binsrch implements the core binary search algorithm used within B-tree pages. The function's behavior differs significantly between internal (non-leaf) and leaf pages to accommodate their different structures and search requirements.

On internal pages, it returns the offset of the last key < scan key (or <= scan key when nextkey is true), which points to the appropriate pivot tuple for descending to the correct child page.

On leaf pages, it handles the final positioning for scans, returning offsets for non-pivot tuples. It supports both forward and backward scan directions, with backward scans requiring special handling to locate the last matching tuple rather than the first.

The algorithm maintains loop invariants based on the nextkey parameter to ensure correct positioning for both ">=" and ">" search semantics.

## Parameters / Member Variables
- : The B-tree index relation being searched
- : BTScanInsert structure containing the search key and search parameters (nextkey, backward, etc.)
- : Buffer containing the page to search

## Dependencies
- Functions called/Symbols referenced:
  - BufferGetPage
  - BTPageGetOpaque
  - P_ISLEAF
  - P_FIRSTDATAKEY
  - PageGetMaxOffsetNumber
  - _bt_compare
  - OffsetNumberPrev
- Called from (representative examples):
  - _bt_search
  - _bt_first

## Notes and Other Information
Key behavioral aspects:
1. **Internal pages**: Returns offset pointing to appropriate pivot tuple for descent
2. **Leaf pages**: Returns offset for actual data positioning in scans
3. **Empty pages**: Handles pages with no keys or only high keys (post-vacuum)
4. **Backward scans**: Special logic to find the last matching tuple rather than first
5. **Search semantics**: Supports both ">=" (nextkey=false) and ">" (nextkey=true) searches
6. **Loop invariants**: Maintains different invariants based on nextkey parameter
7. **No side effects**: Function only examines the page without lock or buffer changes
8. **VACUUM dependency**: Backward scan behavior is relied upon by VACUUM for page re-finding during deletion operations

The function assumes proper page structure and relies on _bt_compare for tuple comparisons. It includes assertions to ensure proper usage patterns, particularly around scantid usage.