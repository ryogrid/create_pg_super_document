# gistRedoPageSplitRecord

## Location
src/backend/access/gist/gistxlog.c: 247 - 341

## Overview
Replays GiST page split operations during WAL recovery, reconstructing multiple pages created during an index page split with proper linking and metadata management.

## Definition
```c
static void gistRedoPageSplitRecord(XLogReaderState *record)
```

## Detailed Description
This function handles the complex WAL recovery process for GiST page splits, which can involve creating multiple new pages from a single original page. Page splits are among the most complex operations in GiST indexes as they require careful coordination of multiple pages and their relationships.

Key operations performed:
1. **Multi-page Processing**: Iterates through all pages involved in the split (xldata->npage)
2. **Root Split Handling**: Special logic for root page splits which create new root pages
3. **Page Initialization**: Clears and initializes each new page with appropriate flags
4. **Tuple Distribution**: Uses decodePageSplitRecord to extract and distribute tuples across pages
5. **Link Management**: Establishes proper right-link chains between pages
6. **Follow-Right Flag Management**: Sets or clears follow-right flags based on split type and position
7. **Lock Management**: Maintains locks on the first page until all operations complete

The function handles two main split scenarios:
- **Root splits**: When the root page is split, creating a new root
- **Non-root splits**: Regular page splits maintaining the existing tree structure

Critical locking protocol: The first page in the split remains locked throughout the entire operation to prevent concurrent access until all pages are properly initialized and linked.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with complete page split information including all pages and their data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extracts gistxlogPageSplit structure)
  - XLogRecGetBlockTag (gets block information for each page)
  - XLogInitBufferForRedo (initializes buffers for new pages)
  - XLogRecGetBlockData (gets serialized tuple data for each page)
  - decodePageSplitRecord (deserializes tuple data)
  - GISTInitBuffer (initializes page structure)
  - gistfillbuffer (fills page with tuples)
  - GistPageGetOpaque, GistPageSetNSN (page metadata management)
  - GistMarkFollowRight, GistClearFollowRight (follow-right flag management)
  - gistRedoClearFollowRight (clears follow-right on child pages)
  - GIST_ROOT_BLKNO, F_LEAF, FirstOffsetNumber (GiST constants)
- Called from (representative examples):
  - gist_redo (main GiST WAL redo dispatcher)

## Notes and Other Information
- This is a static function only used within gistxlog.c
- One of the most complex WAL recovery operations in GiST indexes
- Handles both root and non-root split scenarios with different logic paths
- Critical locking protocol ensures consistency during multi-page operations
- Properly maintains right-link chains and follow-right flags
- Essential for GiST index consistency after crash recovery
- The first page lock is held throughout the entire operation for safety
- Follow-right flag handling differs based on split type and page position
- Part of the comprehensive GiST WAL recovery system
- Requires careful coordination with child page follow-right clearing