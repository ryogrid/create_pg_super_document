# gistbufferinginserttuples

## Location
[src/backend/access/gist/gistbuild.c:1054-1222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L1054-L1222)

## Overview
Inserts tuples to a given page during GiST index buffering-based construction, handling page splits and maintaining parent-child relationships in the internal data structures.

## Definition

```c
static BlockNumber
gistbufferinginserttuples(GISTBuildState *buildstate, Buffer buffer, int level,
						  IndexTuple *itup, int ntup, OffsetNumber oldoffnum,
						  BlockNumber parentblk, OffsetNumber downlinkoffnum)
```
## Detailed Description
This function is analogous to gistinserttuples() in the regular insertion code but operates during the buffering-based index construction phase. It inserts tuples to a specified page and handles the complex logic of page splits, maintaining the parent-child relationship mappings that are crucial for the buffering algorithm.

When a page split occurs, the function recursively inserts downlink tuples to the parent page. For root splits, it updates the root level information and memorizes parent relationships for all affected pages. The function also handles buffer relocation when pages are split, ensuring that buffered tuples are properly redistributed to the correct new pages.

## Parameters / Member Variables
- : GiST build state containing index relation, build buffers, and other construction context
- : Buffer containing the target page for insertion (will be unlocked and unpinned by this function)
- : Level in the tree where insertion is happening (0 for leaf level)
- : Array of index tuples to insert
- : Number of tuples in the itup array
- : Offset number of existing tuple being replaced (if any)
- : Block number of the parent page
- : Offset number of the downlink in the parent page

## Dependencies
- Functions called/Symbols referenced:
  - gistplacetopage
  - BufferGetBlockNumber
  - gistMemorizeAllDownlinks
  - gistMemorizeParent
  - gistBufferingFindCorrectParent
  - gistRelocateBuildBuffersOnSplit
  - PageGetMaxOffsetNumber
  - ReadBuffer
  - UnlockReleaseBuffer
- Called from (representative examples):
  - gistProcessItup
  - gistbufferinginserttuples (recursive calls)

## Notes and Other Information
- This function is static and only used within the GiST buffering build algorithm
- Returns the block number where the first new or updated tuple was placed (usually the original page, but could be a sibling if split occurred)
- Caller must hold a lock on the buffer upon entry; the function will unlock and unpin it
- Handles special case of root splits by updating the root level and memorizing parent relationships for all new child pages
- Uses recursive calls to propagate splits up the tree hierarchy
- Critical for maintaining the parent map data structure used by the buffering algorithm to efficiently locate correct parent pages