# gistBufferingFindCorrectParent

## Location
src/backend/access/gist/gistbuild.c: 1223 - 1296

## Overview
Locates the correct parent page and downlink offset for a given child page during GiST buffering-based index construction.

## Definition


## Detailed Description
This function serves a similar purpose to gistFindCorrectParent() during normal index insertions, but is simplified for the buffering build algorithm since it doesn't need to handle concurrent operations. It locates the downlink tuple in a parent page that points to a specified child page.

The function first attempts to verify the downlink at the previously known location for efficiency. If the downlink has moved, it performs a linear scan of the parent page to locate it. For non-leaf pages (level > 0), it uses the parent map hash table maintained during buffering construction to determine the parent block. For leaf pages, the caller must provide the correct parent block number.

## Parameters / Member Variables
- : GiST build state containing the index relation and parent mapping structures
- : Block number of the child page to find the parent downlink for
- : Tree level of the child page (0 for leaf pages)
- : Input/output parameter for parent block number (updated if found elsewhere)
- : Input/output parameter for downlink offset number (updated when found)

## Dependencies
- Functions called/Symbols referenced:
  - gistGetParent
  - ReadBuffer
  - BufferGetPage
  - LockBuffer
  - gistcheckpage
  - PageGetMaxOffsetNumber
  - PageGetItemId
  - PageGetItem
  - ItemPointerGetBlockNumber
  - OffsetNumberNext
- Called from (representative examples):
  - gistbufferinginserttuples

## Notes and Other Information
- Returns an exclusively-locked buffer containing the parent page with the downlink
- Updates *parentblkno and *downlinkoffnum to reflect the actual location of the downlink
- For leaf pages (level == 0), caller must supply a valid parentblkno or the function will error
- Optimized to check the previously known location first before performing a full page scan
- Simpler than the concurrent version since buffering build has no concurrent access concerns
- Will error if the downlink cannot be found, as this indicates a serious internal inconsistency during build