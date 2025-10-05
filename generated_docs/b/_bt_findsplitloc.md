# _bt_findsplitloc

## Location
[src/backend/access/nbtree/nbtsplitloc.c:129-448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsplitloc.c#L129-L448)

## Overview
Finds an appropriate split point for a B-tree page, balancing space utilization while considering the new item to be inserted and optimizing for suffix truncation effectiveness.

## Definition
```c
OffsetNumber _bt_findsplitloc(Relation rel, Page origpage, OffsetNumber newitemoff, Size newitemsz, IndexTuple newitem, bool *newitemonleft)
```

## Detailed Description
This function determines the optimal location to split a B-tree page when inserting a new item. The primary goal is to equalize free space on both sides of the split after accounting for the new item. For rightmost pages, it applies a fill factor strategy to maintain consistent page density during sequential insertions.

The function implements multiple split strategies:
1. **Default strategy**: Balances space while considering suffix truncation effectiveness on leaf pages
2. **Many duplicates strategy**: Widens the split interval when dealing with many duplicate values
3. **Single value strategy**: Used when all values are identical, favoring high fill factor on the left page

The algorithm evaluates all possible split points, calculates space utilization for each, and selects the optimal point based on the chosen strategy. For leaf pages, it considers suffix truncation benefits by preferring splits that allow more trailing attributes to be truncated from the high key.

## Parameters / Member Variables
- `rel`: B-tree relation being split
- `origpage`: Original page that needs to be split
- `newitemoff`: Offset number where the new item should be inserted
- `newitemsz`: Size of the new item (MAXALIGNED, excluding line pointer)
- `newitem`: The new index tuple to be inserted
- `newitemonleft`: Output parameter indicating whether new item goes on left or right page

## Dependencies
- Functions called/Symbols referenced:
  - BTPageGetOpaque: Get page opaque data
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md): Get maximum offset number
  - [PageGetExactFreeSpace](../P/PageGetExactFreeSpace.md): Calculate available free space
  - BTGetFillFactor: Get relation fill factor
  - [_bt_recsplitloc](_bt_recsplitloc.md): Record potential split locations
  - [_bt_afternewitemoff](_bt_afternewitemoff.md): Check for split-after-new-item optimization
  - [_bt_deltasortsplits](_bt_deltasortsplits.md): Sort split points by delta values
  - [_bt_defaultinterval](_bt_defaultinterval.md): Calculate default split interval
  - [_bt_strategy](_bt_strategy.md): Determine split strategy
  - [_bt_bestsplitloc](_bt_bestsplitloc.md): Select best split point from candidates
- Called from:
  - [_bt_split](_bt_split.md): Main page splitting function

## Notes and Other Information
- Returns the offset number of the first tuple that should go on the right page
- The function never fails to find a feasible split point, but includes error handling for safety
- Special handling for rightmost pages to maintain consistent fill factors during sequential insertions
- Considers posting list items and ensures newitem cannot be a posting list item
- The split location affects both space utilization and suffix truncation effectiveness on leaf pages
- Uses different fill factor strategies for leaf vs non-leaf pages

## Simplified Source
```c
OffsetNumber
_bt_findsplitloc(Relation rel, Page origpage, OffsetNumber newitemoff,
                 Size newitemsz, IndexTuple newitem, bool *newitemonleft)
{
    BTPageOpaque opaque = BTPageGetOpaque(origpage);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(origpage);
    FindSplitData state;

    // Calculate available space on left and right pages
    int leftspace = rightspace =
        PageGetPageSize(origpage) - SizeOfPageHeaderData -
        MAXALIGN(sizeof(BTPageOpaqueData));

    // Account for high key on right page
    if (!P_RIGHTMOST(opaque)) {
        ItemId itemid = PageGetItemId(origpage, P_HIKEY);
        rightspace -= MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData);
    }

    // Initialize split state
    state.rel = rel;
    state.origpage = origpage;
    state.newitem = newitem;
    state.newitemsz = newitemsz + sizeof(ItemIdData);
    state.is_leaf = P_ISLEAF(opaque);
    state.is_rightmost = P_RIGHTMOST(opaque);
    state.leftspace = leftspace;
    state.rightspace = rightspace;
    state.olddataitemstotal = rightspace - PageGetExactFreeSpace(origpage);
    state.newitemoff = newitemoff;

    // Allocate space for candidate split points
    state.maxsplits = maxoff;
    state.splits = palloc(sizeof(SplitPoint) * state.maxsplits);
    state.nsplits = 0;

    // Scan through existing items and record potential split points
    int olddataitemstoleft = 0;
    for (OffsetNumber offnum = P_FIRSTDATAKEY(opaque);
         offnum <= maxoff;
         offnum = OffsetNumberNext(offnum))
    {
        ItemId itemid = PageGetItemId(origpage, offnum);
        Size itemsz = MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData);

        // Record split points relative to newitemoff position
        if (offnum < newitemoff)
            _bt_recsplitloc(&state, offnum, false, olddataitemstoleft, itemsz);
        else if (offnum > newitemoff)
            _bt_recsplitloc(&state, offnum, true, olddataitemstoleft, itemsz);
        else {
            // At newitemoff - record splits before and after newitem
            _bt_recsplitloc(&state, offnum, false, olddataitemstoleft, itemsz);
            _bt_recsplitloc(&state, offnum, true, olddataitemstoleft, itemsz);
        }

        olddataitemstoleft += itemsz;
    }

    // Record split after all existing items if newitem goes at end
    if (newitemoff > maxoff)
        _bt_recsplitloc(&state, newitemoff, false, state.olddataitemstotal, 0);

    // Determine fill factor strategy based on page type and position
    double fillfactormult;
    bool usemult;

    if (!state.is_leaf) {
        // Non-leaf page
        usemult = state.is_rightmost;
        fillfactormult = BTREE_NONLEAF_FILLFACTOR / 100.0;
    } else if (state.is_rightmost) {
        // Rightmost leaf page
        usemult = true;
        fillfactormult = BTGetFillFactor(rel) / 100.0;
    } else if (_bt_afternewitemoff(&state, maxoff, BTGetFillFactor(rel), &usemult)) {
        // Split-after-newitem optimization
        if (usemult) {
            fillfactormult = BTGetFillFactor(rel) / 100.0;
        } else {
            // Find exact split point after newitem
            for (int i = 0; i < state.nsplits; i++) {
                SplitPoint *split = state.splits + i;
                if (split->newitemonleft && newitemoff == split->firstrightoff) {
                    pfree(state.splits);
                    *newitemonleft = true;
                    return newitemoff;
                }
            }
            fillfactormult = 0.50;
        }
    } else {
        // Regular leaf page - 50:50 split
        usemult = false;
        fillfactormult = 0.50;
    }

    // Sort split points by delta from ideal fill factor
    _bt_deltasortsplits(&state, fillfactormult, usemult);

    // Determine split strategy and interval
    state.interval = _bt_defaultinterval(&state);
    SplitPoint leftpage = state.splits[0];
    SplitPoint rightpage = state.splits[state.nsplits - 1];

    FindSplitStrat strategy;
    int perfectpenalty = _bt_strategy(&state, &leftpage, &rightpage, &strategy);

    // Adjust strategy if needed
    if (strategy == SPLIT_MANY_DUPLICATES) {
        state.interval = state.nsplits;  // Consider all split points
    } else if (strategy == SPLIT_SINGLE_VALUE) {
        // Single value - split near end of page
        usemult = true;
        fillfactormult = BTREE_SINGLEVAL_FILLFACTOR / 100.0;
        _bt_deltasortsplits(&state, fillfactormult, usemult);
        state.interval = 1;
    }

    // Find the best split point from acceptable candidates
    OffsetNumber firstrightoff = _bt_bestsplitloc(&state, perfectpenalty,
                                                  newitemonleft, strategy);
    pfree(state.splits);

    return firstrightoff;
}
```