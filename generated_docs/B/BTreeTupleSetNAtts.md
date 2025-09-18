# BTreeTupleSetNAtts

## Location
src/include/access/nbtree.h: 595 - 619

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
  - BTreeTupleIsPivot
  - INDEX_ALT_TID_MASK
  - BT_PIVOT_HEAP_TID_ATTR
  - ItemPointerSetOffsetNumber
- Called from (representative examples):
  - _bt_newlevel
  - _bt_pgaddtup
  - _bt_sortaddtup
  - _bt_buildadd
  - _bt_truncate
  - BTreeTupleSetTopParent

## Notes and Other Information
- This function transforms a regular index tuple into a pivot tuple by setting appropriate metadata bits
- The BT_IS_POSTING bit is deliberately left unset, as indicated in the comment
- Includes comprehensive assertions to validate input parameters and ensure pivot tuple invariants
- The heaptid parameter enables the special pivot tuple representation that includes heap TID tiebreaker values
- Used during B-tree construction, page splitting, and tuple truncation operations
- The function establishes the foundation for pivot tuple functionality in B-tree internal pages