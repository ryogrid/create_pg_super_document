# _bt_check_natts

## Location
src/backend/access/nbtree/nbtutils.c: 4923 - 5082

## Overview
Verifies that a tuple at a specific offset on a B-tree page has the expected number of attributes, serving as a comprehensive sanity check for various B-tree tuple types and index configurations.

## Definition
```c
bool _bt_check_natts(Relation rel, bool heapkeyspace, Page page, OffsetNumber offnum)
```

## Detailed Description
This function performs strict validation of tuple attribute counts based on the B-tree page type, tuple position, and index configuration. It handles multiple scenarios including regular leaf tuples, pivot tuples, negative infinity tuples, and posting list tuples.

The function is designed to be as strict as possible about attribute count expectations for each version of B-tree implementation. It handles both heapkeyspace and non-heapkeyspace indexes, accounting for differences in tuple representation between PostgreSQL versions (particularly the pre-v11 format for negative infinity tuples).

The validation logic differs significantly based on:
- Whether the page is a leaf or internal page
- The position of the tuple on the page (high key, negative infinity, regular data)
- Index configuration (heapkeyspace vs non-heapkeyspace, presence of INCLUDE columns)
- Tuple type (pivot, posting list, regular)

## Parameters / Member Variables
- `rel`: The index relation being validated
- `heapkeyspace`: Whether the index uses heapkeyspace semantics (v11+ format)
- `page`: The B-tree page containing the tuple to check
- `offnum`: The offset number of the tuple to validate

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfAttributes
  - IndexRelationGetNumberOfKeyAttributes
  - BTPageGetOpaque
  - P_IGNORE
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)/PageGetItemId
  - BTreeTupleGetNAtts
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [BTreeTupleGetHeapTID](../B/BTreeTupleGetHeapTID.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - P_ISLEAF, P_FIRSTDATAKEY, P_RIGHTMOST, P_HIKEY
- Called from (representative examples):
  - [_bt_compare](_bt_compare.md)
  - Various B-tree verification routines

## Notes and Other Information
- Cannot reliably test deleted or half-dead pages due to dummy high keys
- Non-heapkeyspace indexes do not support deduplication (posting list tuples)
- INCLUDE indexes do not support deduplication
- Posting list tuples must not have the "pivot heap TID" bit set
- Negative infinity tuples are always truncated to zero attributes in heapkeyspace indexes
- Pre-v11 negative infinity tuples may use P_HIKEY as offset number for validation
- Pivot tuples must be explicitly marked and cannot use posting list representation
- Heap TID cannot be untruncated when other key attributes are truncated
- Pivot tuples must have at least one untruncated key attribute (except minus infinity)
- Preferred alternative: BTreeTupleGetNAtts() for direct tuple testing when context allows