# comparetup_index_hash

## Location
src/backend/utils/sort/tuplesortvariants.c: 1588 - 1663

## Overview
A specialized comparison function for hash index sorting that orders tuples by bucket number, hash value, and ItemPointer to optimize insertion and scan performance.

## Definition


## Detailed Description
This function implements a three-level comparison strategy specifically designed for hash index tuple sorting:

1. **Bucket-level comparison**: First compares bucket numbers computed from hash values using hash index parameters (max_buckets, high_mask, low_mask). This ensures tuples are grouped by their target bucket.

2. **Hash value comparison**: When bucket numbers are equal, compares the actual hash values to maintain hash ordering within each bucket. This enables efficient binary search within bucket/overflow pages.

3. **ItemPointer tiebreaking**: When hash values are also equal, uses heap TID as the final comparison criterion to ensure deterministic ordering and potentially improve scan locality.

The function assumes the first column of the index tuple contains the hash key and that it is never NULL (enforced by assertions).

## Parameters / Member Variables
- : First SortTuple to compare containing an IndexTuple with hash key in first column
- : Second SortTuple to compare containing an IndexTuple with hash key in first column
- : Tuplesortstate containing hash index-specific configuration via TuplesortIndexHashArg

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [_hash_hashkey2bucket](../h/_hash_hashkey2bucket.md)
  - [DatumGetUInt32](../D/DatumGetUInt32.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
- Called from (representative examples):
  - [tuplesort_begin_index_hash](../t/tuplesort_begin_index_hash.md)
  - CLUSTER_SORT

## Notes and Other Information
- The function requires that hash keys are never NULL (enforced by assertions)
- Bucket computation uses hash index split algorithm parameters (max_buckets, high_mask, low_mask)
- The ordering by hash value within buckets supports fast binary search on bucket/overflow pages
- ItemPointer comparison provides deterministic ordering and may improve physical scan locality
- This comparison function is specifically optimized for hash index building and does not apply to other index types
- The final Assert(false) indicates that valid tuples should never have identical ItemPointers