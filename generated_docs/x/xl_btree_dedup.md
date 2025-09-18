# xl_btree_dedup

## Location
src/include/access/nbtxlog.h: 170 - 175

## Overview
The xl_btree_dedup structure represents a WAL record for B-tree deduplication operations, which merge consecutive tuples with equal keys into posting list tuples to save space.

## Definition
```c
typedef struct xl_btree_dedup
{
    uint16      nintervals;
    
    /* DEDUPLICATION INTERVALS FOLLOW */
} xl_btree_dedup;
```

## Detailed Description
This structure logs B-tree page deduplication passes, an optimization technique used to reduce page space consumption by merging duplicate key values. When a leaf page contains consecutive groups of tuples with identical keys, deduplication converts them into posting list tuples where a single key points to multiple tuple identifiers (TIDs).

The WAL record contains the number of deduplication intervals and is followed by an array of BTDedupInterval structures that define which ranges of tuples should be merged. This allows recovery to replay the exact same deduplication operation, ensuring consistency between the original operation and recovery.

## Parameters / Member Variables
- `nintervals`: The number of BTDedupInterval structures that follow this record, indicating how many deduplication operations were performed

## Dependencies
- Functions called/Symbols referenced:
  - uint16 (type)
  - BTDedupInterval (struct array that follows)

- Called from (representative examples):
  - _bt_dedup_pass (src/backend/access/nbtree/nbtdedup.c:249)
  - btree_xlog_dedup (src/backend/access/nbtree/nbtxlog.c:467)
  - btree_desc (src/backend/access/rmgrdesc/nbtdesc.c:53)
  - SizeOfBtreeDedup (src/include/access/nbtxlog.h:177)

## Notes and Other Information
- Only applies to leaf pages since posting lists are a leaf-page-only optimization
- The deduplication process groups consecutive tuples with equal keys into single posting list tuples
- Each BTDedupInterval in the following array specifies a range of consecutive equal-key tuples to merge
- This optimization significantly reduces space usage in indexes with many duplicate key values
- Recovery uses the interval information to recreate the exact same posting lists that were created during the original operation
- Deduplication is a space optimization that doesn't affect the logical content of the index, only its physical representation