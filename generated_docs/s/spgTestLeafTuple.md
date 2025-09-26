# spgTestLeafTuple

## Location
[src/backend/access/spgist/spgscan.c:763-816](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L763-L816)

## Overview
Tests a leaf tuple during SP-GiST index scanning, handling various tuple states and performing leaf-specific consistency checks.

## Definition
static OffsetNumber spgTestLeafTuple(SpGistScanOpaque so, SpGistSearchItem *item, Page page, OffsetNumber offset, bool isnull, bool isroot, bool *reportedSome, storeRes_func storeRes)

## Detailed Description
This function is responsible for processing leaf tuples during SP-GiST index scans. It handles different tuple states including live, redirect, and dead tuples. For live tuples, it validates the heap pointer and calls spgLeafTest to perform the actual consistency check against scan keys. For redirect tuples, it updates the search item to point to the redirect target. For dead tuples, it signals to break the chain. The function ensures proper handling of tuple chains and maintains scan integrity by validating tuple states and positions.

## Parameters / Member Variables
- : SpGistScanOpaque structure containing scan state and context
- : SpGistSearchItem representing the current search item being processed
- : Page containing the leaf tuple to be tested
- : OffsetNumber indicating the position of the tuple on the page
- : Boolean indicating if the tuple value is null
- : Boolean indicating if this tuple is on the root page
- : Boolean pointer indicating if any results have been reported
- : Function pointer for storing scan results

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - [spgLeafTest](spgLeafTest.md)
  - SGLT_GET_NEXTOFFSET
  - [SpGistLeafTuple](../S/SpGistLeafTuple.md)
  - SpGistDeadTuple
- Called from (representative examples):
  - [spgWalk](spgWalk.md)

## Notes and Other Information
- This is a static function internal to spgscan.c
- Returns different OffsetNumber values based on tuple state: next offset for live tuples, SpGistRedirectOffsetNumber for redirects, SpGistBreakOffsetNumber for dead chains, or SpGistErrorOffsetNumber for errors
- Handles tuple state transitions and validates chain integrity
- All tuples on root pages are expected to be live
- Located at src/backend/access/spgist/spgscan.c:763-816