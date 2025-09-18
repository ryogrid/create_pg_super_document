# brin_doupdate

## Location
src/backend/access/brin/brin_pageops.c: 53 - 322

## Overview
Updates a BRIN (Block Range Index) tuple by replacing an existing tuple with a new one, handling both same-page and cross-page updates with proper WAL logging and revmap maintenance.

## Definition


## Detailed Description
The  function performs atomic updates of BRIN index tuples, which represent summarized information about ranges of heap blocks. The function handles two main scenarios:

1. **Same-page update**: When the new tuple fits in the same page as the original tuple, it performs an in-place replacement using .

2. **Cross-page update**: When there's insufficient space on the original page, it removes the old tuple and inserts the new tuple on a different page, updating the revmap to maintain the mapping from heap block ranges to index tuples.

The function includes comprehensive validation to detect concurrent modifications, ensures proper WAL logging for crash recovery, and manages buffer locking to maintain consistency. It also handles page extension when necessary and updates the free space map appropriately.

## Parameters / Member Variables
- : The BRIN index relation being updated
- : Number of heap pages covered by each BRIN tuple
- : Reverse mapping structure that tracks heap block to index tuple mappings
- : Starting heap block number for the range being updated
- : Buffer containing the page with the original tuple
- : Offset number of the original tuple within the page
- : Pointer to the original tuple (used for validation)
- : Size of the original tuple
- : Pointer to the new tuple to be inserted
- : Size of the new tuple
- : Boolean flag indicating whether to attempt same-page update

## Dependencies
- Functions called/Symbols referenced:
  - : Extends revmap to cover the required heap block
  - : Finds a suitable buffer for tuple insertion
  - : Checks if same-page update is possible
  - : Validates tuple equality for concurrency control
  - : Performs in-place tuple replacement
  - : Locks revmap page for atomic updates
  - : Updates revmap with new tuple location
  - : WAL logging for crash recovery
- Called from (representative examples):
  - : Main BRIN insertion function
  - : Range summarization during index maintenance
  - : Page evacuation during vacuum operations

## Notes and Other Information
- The function returns  on successful update,  if the update should be retried
- Implements proper concurrency control by validating that the original tuple hasn't been modified
- Handles page evacuation flags and ensures evacuated pages are not used for same-page updates
- Includes comprehensive WAL logging with different record types for same-page vs cross-page updates
- Manages free space map updates when new pages are allocated
- Uses critical sections to ensure atomicity of multi-buffer operations
- Validates tuple size limits and returns appropriate errors for oversized tuples