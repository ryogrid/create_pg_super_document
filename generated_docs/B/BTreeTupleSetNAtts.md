# BTreeTupleSetNAtts

## Location
[src/include/access/nbtree.h:595-619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L595-L619)

## Overview
Sets the number of key attributes in an index tuple and optionally marks it to include a heap TID tiebreaker, configuring the tuple as a pivot tuple with the specified attribute count.

## Definition
static inline void BTreeTupleSetNAtts(IndexTuple itup, uint16 nkeyatts, bool heaptid)

## Detailed Description
This function configures an index tuple to be a pivot tuple by setting the number of key attributes and optionally indicating that a heap TID tiebreaker attribute will be stored. The function manipulates the tuple's metadata fields to establish its role as a pivot tuple in the B-tree structure.

The function sets the INDEX_ALT_TID_MASK bit to indicate this is a pivot tuple, then stores the attribute count in the offset number field of the tuple's ItemPointer. If heaptid is true, it sets the BT_PIVOT_HEAP_TID_ATTR bit to indicate that a heap TID value follows the key attributes.

Several assertions ensure data integrity: the attribute count doesn't exceed INDEX_MAX_KEYS, doesn't conflict with status bits, and follows proper pivot tuple semantics.

## Parameters / Member Variables
- itup: IndexTuple to configure as a pivot tuple
- nkeyatts: Number of key attributes in the tuple (must be <= INDEX_MAX_KEYS)
- heaptid: Boolean indicating whether a heap TID tiebreaker attribute should be included

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS
  - BT_STATUS_OFFSET_MASK
  - [BTreeTupleIsPivot](BTreeTupleIsPivot.md)
  - INDEX_ALT_TID_MASK
  - BT_PIVOT_HEAP_TID_ATTR
  - [ItemPointerSetOffsetNumber](../I/ItemPointerSetOffsetNumber.md)
- Called from (representative examples):
  - [_bt_newlevel](../b/_bt_newlevel.md)
  - [_bt_pgaddtup](../b/_bt_pgaddtup.md)
  - [_bt_sortaddtup](../b/_bt_sortaddtup.md)
  - [_bt_buildadd](../b/_bt_buildadd.md)
  - [_bt_truncate](../b/_bt_truncate.md)
  - [BTreeTupleSetTopParent](BTreeTupleSetTopParent.md)

## Notes and Other Information
- This function transforms a regular index tuple into a pivot tuple by setting appropriate metadata bits
- The BT_IS_POSTING bit is deliberately left unset, as indicated in the comment
- Includes comprehensive assertions to validate input parameters and ensure pivot tuple invariants
- The heaptid parameter enables the special pivot tuple representation that includes heap TID tiebreaker values
- Used during B-tree construction, page splitting, and tuple truncation operations
- The function establishes the foundation for pivot tuple functionality in B-tree internal pages

## Simplified Source

```c
static inline void
BTreeTupleSetNAtts(IndexTuple itup, uint16 nkeyatts, bool heaptid)
{
    // Validate input parameters
    Assert(nkeyatts <= INDEX_MAX_KEYS);
    Assert((nkeyatts & BT_STATUS_OFFSET_MASK) == 0);
    Assert(!heaptid || nkeyatts > 0);
    Assert(!BTreeTupleIsPivot(itup) || nkeyatts == 0);

    // Mark tuple as pivot tuple
    itup->t_info |= INDEX_ALT_TID_MASK;

    // Set heap TID tiebreaker bit if requested
    if (heaptid)
        nkeyatts |= BT_PIVOT_HEAP_TID_ATTR;

    // Store attribute count in offset number field
    // BT_IS_POSTING bit is deliberately unset here
    ItemPointerSetOffsetNumber(&itup->t_tid, nkeyatts);

    Assert(BTreeTupleIsPivot(itup));
}
```