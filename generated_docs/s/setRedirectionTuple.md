# setRedirectionTuple

## Location
src/backend/access/spgist/spgdoinsert.c: 568 - 598

## Overview
This function updates a previously-created redirection tuple with the appropriate destination block and offset when the final destination wasn't known at creation time.

## Definition


## Detailed Description
This utility function modifies an existing SPGIST_REDIRECT tuple to point to its correct final destination. It's used when redirection tuples are initially created with a placeholder destination (the metapage) because the actual destination isn't known at creation time. The function:

1. Retrieves the dead tuple at the specified position on the page
2. Verifies it's actually a redirect tuple pointing to the metapage (placeholder)
3. Updates the ItemPointer to point to the correct destination block and offset

This pattern is used during SPGiST operations where tuples may need to be moved or split, but the final locations are determined later in the process.

## Parameters / Member Variables
- : Page descriptor for the page containing the redirection tuple to update
- : The offset number of the redirection tuple on the page
- : The destination block number to set in the redirection pointer
- : The destination offset number to set in the redirection pointer

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItem](../P/PageGetItem.md)/PageGetItemId (page item access functions)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md) (gets block number from item pointer)
  - [ItemPointerSet](../I/ItemPointerSet.md) (sets block and offset in item pointer)
  - SPGIST_METAPAGE_BLKNO (constant for metapage block number used as placeholder)
- Called from (representative examples):
  - [doPickSplit](../d/doPickSplit.md) (at src/backend/access/spgist/spgdoinsert.c:1286)
  - [doPickSplit](../d/doPickSplit.md) (at src/backend/access/spgist/spgdoinsert.c:1323)

## Notes and Other Information
- The function includes assertions to verify the tuple is actually a SPGIST_REDIRECT tuple
- Also verifies the tuple currently points to the metapage (SPGIST_METAPAGE_BLKNO) as expected
- This is a utility function that operates on an existing tuple without WAL logging
- Used as part of larger operations that handle their own WAL logging
- The "impossible" destination of the metapage serves as a placeholder until the real destination is determined
- Location: src/backend/access/spgist/spgdoinsert.c:568-598