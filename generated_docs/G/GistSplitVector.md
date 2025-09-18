# GistSplitVector

## Location
src/include/access/gist_private.h: 235 - 249

## Overview
GistSplitVector is a working state structure used during GiST index page splitting operations to manage multi-column split logic and coordinate with user-defined PickSplit methods.

## Definition


## Detailed Description
GistSplitVector serves as the working state container for complex page splitting operations in GiST indexes. It extends the basic GIST_SPLITVEC interface by providing additional bookkeeping for multi-column indexes. The structure manages the union keys (bounding boxes) for both sides of the split and tracks tuples that could be placed on either side without penalty, enabling optimization of the split quality. This structure is particularly important for composite indexes where multiple attributes contribute to the index key.

## Parameters / Member Variables
- : GIST_SPLITVEC structure passed to and from user-defined PickSplit methods
- : Array of Datum values representing the union of subkeys for the left split page, sized by INDEX_MAX_KEYS
- : Array of boolean flags indicating which subkeys in spl_lattr are null
- : Array of Datum values representing the union of subkeys for the right split page, sized by INDEX_MAX_KEYS
- : Array of boolean flags indicating which subkeys in spl_rattr are null
- : Pointer to boolean array flagging tuples that could be placed on either side of the split with zero penalty

## Dependencies
- Functions called/Symbols referenced:
  - GIST_SPLITVEC
  - INDEX_MAX_KEYS
- Called from (representative examples):
  - gistSplit
  - gistunionsubkey
  - findDontCares
  - placeOne
  - gistUserPicksplit
  - gistSplitByKey

## Notes and Other Information
The structure's design reflects the complexity of optimizing GiST splits for multi-dimensional and composite data types. The spl_dontcare mechanism is a key optimization that allows the split algorithm to place certain tuples on either side of the split, which can be used to balance page utilization or minimize overlap between resulting pages. This flexibility is crucial for maintaining good index performance in high-dimensional spaces where traditional split strategies might result in poor page utilization.