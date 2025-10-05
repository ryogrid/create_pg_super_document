# _bt_posting_valid

## Location
[src/backend/access/nbtree/nbtdedup.c:1078-1105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L1078-L1105)

## Overview
Validates that a posting list tuple is correctly structured with properly ordered heap TIDs in ascending order.

## Definition

```c
static bool
_bt_posting_valid(IndexTuple posting)
```
## Detailed Description
This function performs comprehensive validation of a B-tree posting list tuple to ensure data integrity. It verifies that the tuple is indeed a posting list type, contains at least 2 heap TIDs (minimum required for a posting list), and that all TIDs are valid and arranged in strict ascending order.

The validation process includes:
1. Type checking to confirm the tuple is a posting list
2. Minimum size validation (posting lists must have at least 2 TIDs)
3. Individual TID validity checks using ItemPointerIsValid
4. Ordering verification ensuring each TID is strictly greater than the previous one

This function is critical for maintaining B-tree index integrity and is used extensively during debugging and assertion checking throughout the posting list manipulation code.

## Parameters / Member Variables
- `posting`: The IndexTuple to validate as a proper posting list
## Dependencies
- Functions called/Symbols referenced:
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [BTreeTupleGetNPosting](../B/BTreeTupleGetNPosting.md)
  - [BTreeTupleGetHeapTID](../B/BTreeTupleGetHeapTID.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
  - [BTreeTupleGetPostingN](../B/BTreeTupleGetPostingN.md)
  - [ItemPointerCompare](../I/ItemPointerCompare.md)
- Called from (representative examples):
  - [_bt_bottomupdel_finish_pending](_bt_bottomupdel_finish_pending.md)
  - [_bt_form_posting](_bt_form_posting.md)
  - [_bt_update_posting](_bt_update_posting.md)
  - [_bt_swap_posting](_bt_swap_posting.md)

## Notes and Other Information
- Returns false if the tuple is not a posting list or has fewer than 2 TIDs
- Returns false if any TID is invalid or if TIDs are not in strict ascending order
- This is a static function used internally within the nbtdedup.c module
- Primarily used in assertion checks and debugging to ensure posting list integrity
- The strict ordering requirement is essential for binary search operations on posting lists
- Performance note: O(n) complexity where n is the number of TIDs in the posting list

## Simplified Source

```c
static bool _bt_posting_valid(IndexTuple posting) {
    // Basic type and size validation
    if (!BTreeTupleIsPosting(posting) || BTreeTupleGetNPosting(posting) < 2)
        return false;

    // Get first TID as baseline for comparison
    ItemPointerData last;
    ItemPointerCopy(BTreeTupleGetHeapTID(posting), &last);
    if (!ItemPointerIsValid(&last))
        return false;

    // Validate each subsequent TID is valid and in ascending order
    for (int i = 1; i < BTreeTupleGetNPosting(posting); i++) {
        ItemPointer htid = BTreeTupleGetPostingN(posting, i);

        if (!ItemPointerIsValid(htid))
            return false;

        if (ItemPointerCompare(htid, &last) <= 0)
            return false; // Must be strictly ascending

        ItemPointerCopy(htid, &last); // Update baseline for next comparison
    }

    return true; // All validations passed
}
```