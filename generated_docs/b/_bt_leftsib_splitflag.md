# _bt_leftsib_splitflag

## Location
[src/backend/access/nbtree/nbtpage.c:1695-1751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1695-L1751)

## Overview
Checks whether the left sibling page of a target page is marked with the INCOMPLETE_SPLIT flag, which would prevent safe deletion of the target page.

## Definition


## Detailed Description
This function is used during B-tree page deletion operations to verify that the left sibling page is in a consistent state for deletion to proceed. It checks whether the left sibling page has the  flag set, which would indicate that a page split operation affecting the target page is still incomplete.

The function performs several important validations:
1. **Split completion check**: Verifies that any split operation that created the target page has been completed
2. **Sibling consistency**: Ensures the left sibling's next-pointer still points to the target page
3. **Deletion safety**: Determines if it's safe to proceed with target page deletion

If the left sibling has an incomplete split flag set AND its next-pointer points to the target, this indicates the target page doesn't have a proper downlink in its parent page. The page deletion algorithm cannot handle this scenario safely, so deletion must be postponed until the split is completed.

The function includes logic to handle concurrent splits: if the left sibling was split concurrently such that it no longer points to the target page, the original split that created the target must have been completed, making deletion safe to proceed.

## Parameters / Member Variables
- : The B-tree index relation being operated on
- : Block number of the left sibling page to check
- : Block number of the target page being considered for deletion

## Dependencies
- Functions called/Symbols referenced:
  - : Acquires a buffer for the left sibling page with read lock
  - : Releases the buffer for the left sibling page
  - : Gets the opaque data structure from the page
  - : Macro to check if the incomplete split flag is set
  - : Constant representing an invalid/null block number
  - : Lock mode constant for read access
- Called from:
  - : Main page deletion function
  - : Subtree parent locking during deletion

## Notes and Other Information
- Returns  if the left sibling has INCOMPLETE_SPLIT flag set and points to target (deletion unsafe)
- Returns  if no left sibling exists (leftsib == P_NONE) or if deletion is safe to proceed
- The caller should not hold a lock on the target page to avoid deadlocks (pages must be locked left-to-right)
- This check is essential for maintaining B-tree structural integrity during deletion operations
- The function handles the case where there is no left sibling (target is leftmost page) by returning false
- Concurrent split detection ensures that completed splits don't prevent legitimate deletions