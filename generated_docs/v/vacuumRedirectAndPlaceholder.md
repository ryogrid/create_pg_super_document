# vacuumRedirectAndPlaceholder

## Location
src/backend/access/spgist/spgvacuum.c: 493 - 620

## Overview
Cleans up redirect and placeholder tuples on SP-GiST pages by converting old redirections to placeholders and removing trailing placeholder tuples that don't affect offset numbering.

## Definition
```c
static void vacuumRedirectAndPlaceholder(Relation index, Relation heaprel, Buffer buffer)
```

## Detailed Description
This function performs cleanup of redirect and placeholder tuples on both leaf and inner SP-GiST pages. It operates in two main phases:

**Phase 1 - Redirect to Placeholder Conversion**:
- Scans backwards through the page looking for REDIRECT tuples
- Converts REDIRECT tuples to PLACEHOLDER if they are old enough (no active transactions can see them)
- Uses global visibility testing to determine if redirects can be safely converted
- Tracks the newest XID among converted redirects for snapshot conflict handling

**Phase 2 - Placeholder Removal**:
- Identifies trailing placeholder tuples that can be safely removed
- Only removes placeholders at the end of the page to avoid changing offset numbers of non-placeholder tuples
- Uses bulk deletion for efficiency since the trailing placeholders are in sequential order

The function maintains SP-GiST page statistics (nRedirection, nPlaceholder) and handles both regular and logical decoding catalog relations. All operations are performed within a critical section with proper WAL logging.

## Parameters / Member Variables
- `index`: The SP-GiST index relation being processed
- `heaprel`: The heap relation associated with the index (used for visibility testing)
- `buffer`: Buffer containing the page to clean up (works on both leaf and inner pages)

## Dependencies
- Functions called/Symbols referenced:
  - GlobalVisTestFor, GlobalVisTestIsRemovableXid: Visibility testing functions
  - RelationIsAccessibleInLogicalDecoding: Logical decoding support check
  - PageIndexMultiDelete: Bulk tuple deletion
  - SpGistPageGetOpaque: Page opaque data access
  - TransactionIdIsValid, TransactionIdPrecedes: Transaction ID operations
  - XLog functions: WAL logging (XLogBeginInsert, XLogInsert, etc.)
  - ItemPointerSetInvalid: Invalidates redirect target pointer
- Called from (representative examples):
  - spgvacuumpage: Called for both leaf and inner pages during regular vacuum
  - spgprocesspending: Called when processing pages from pending list

## Notes and Other Information
- This is a static function within the spgvacuum.c file
- Unlike other vacuum functions, this works on both leaf and inner pages
- Uses backward scanning to efficiently identify trailing placeholders
- Maintains offset number stability by only removing trailing placeholders
- Includes support for logical decoding through catalog relation handling
- Uses global visibility state for safe redirect-to-placeholder conversion
- Updates page statistics counters (nRedirection, nPlaceholder) maintained in page opaque data
- Performs all operations within critical sections for crash safety
- WAL logging includes snapshot conflict horizon for standby query handling
- The function is conservative in redirect conversion - only converts when definitely safe
- Part of the SP-GiST vacuum subsystem focused on cleaning up non-live tuples
- Handles both transaction visibility and logical decoding requirements
- The backward scan optimization allows efficient identification of removable trailing placeholders