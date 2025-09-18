# btvacuumpage

## Location
src/backend/access/nbtree/nbtree.c: 1073 - 1407

## Overview
Processes a single page during B-tree vacuum operations, handling deletions, updates to posting list tuples, and page splits that occurred during the vacuum cycle.

## Definition


## Detailed Description
The  function is the core page-processing routine for B-tree vacuum operations. It handles a single page identified by  during a vacuum scan. The function manages complex scenarios including:

1. **Page recycling**: Identifying and recycling deleted or empty pages
2. **Tuple deletion**: Removing dead tuples from leaf pages based on callback results
3. **Posting list updates**: Handling partial deletions in posting list tuples (where some TIDs are dead but others remain live)
4. **Page split handling**: Detecting and handling page splits that occurred after the vacuum cycle began by backtracking to process sibling pages
5. **Half-dead page cleanup**: Finishing deletion of pages left in half-dead state by interrupted vacuum operations

The function implements a sophisticated backtracking mechanism to ensure that page splits occurring during the vacuum don't cause tuples to be missed. When a page split moves tuples to a block number lower than the current scan position, the function backtracks to process those pages.

## Parameters / Member Variables
- : BTVacState structure containing vacuum state information including callback function, statistics, and cycle ID
- : Block number of the page to vacuum, which may differ from the actual page being processed during backtracking

## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [_bt_lockbuf](_bt_lockbuf.md)/_bt_relbuf
  - [_bt_checkpage](_bt_checkpage.md)
  - BTPageGetOpaque
  - [BTPageIsRecyclable](../B/BTPageIsRecyclable.md)
  - RecordFreeIndexPage
  - [_bt_upgradelockbufcleanup](_bt_upgradelockbufcleanup.md)
  - [btreevacuumposting](btreevacuumposting.md)
  - [_bt_delitems_vacuum](_bt_delitems_vacuum.md)
  - [_bt_pagedel](_bt_pagedel.md)
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)/BTreeTupleIsPivot
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md)
- Called from:
  - [btvacuumscan](btvacuumscan.md) (main vacuum scan function)

## Notes and Other Information
- Uses a 'goto backtrack' mechanism to handle page splits that occurred during vacuum
- Maintains detailed statistics about deleted tuples, pages, and TIDs
- Implements memory management using temporary contexts for page deletion operations  
- Handles both regular tuples and posting list tuples (which contain multiple TIDs)
- Updates btpo_cycleid to prevent reprocessing of split pages
- Critical for maintaining B-tree index integrity during vacuum operations