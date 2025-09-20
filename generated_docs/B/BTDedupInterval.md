# BTDedupInterval

## Location
[src/include/access/nbtree.h:841-845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L841-L845)

## Overview
BTDedupInterval is a structure used to represent an individual pending tuple during B-tree deduplication operations.

## Definition

```c
typedef struct BTDedupInterval
{
	OffsetNumber baseoff;
	uint16		nitems;
} BTDedupInterval;
```
## Detailed Description
BTDedupInterval serves as a compact representation of a range of tuples that are candidates for deduplication within a B-tree page. During deduplication operations, the algorithm identifies intervals of duplicate tuples that can be consolidated into posting list tuples. Each BTDedupInterval represents one such interval by storing the starting offset and the number of consecutive items that share the same key values.

This structure is fundamental to PostgreSQL's B-tree deduplication feature, which helps reduce index bloat by combining multiple tuples with identical key values into a single posting list tuple. The interval-based approach allows for efficient batch processing of duplicate tuples during deduplication passes.

## Parameters / Member Variables
- `baseoff`: The starting OffsetNumber (base offset) of the interval of tuples to be deduplicated
- `nitems`: The number of consecutive items in the interval that can be deduplicated together
## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (type)
  - uint16 (type)
- Called from (representative examples):
  - [_bt_dedup_pass](../b/_bt_dedup_pass.md)
  - [btree_xlog_dedup](../b/btree_xlog_dedup.md)
  - [BTDedupStateData](BTDedupStateData.md) (as member)

## Notes and Other Information
- Used primarily in B-tree deduplication algorithms to efficiently track ranges of duplicate tuples
- The structure is designed to be lightweight to minimize memory overhead during deduplication operations
- Essential component of PostgreSQL's index space optimization through tuple deduplication
- Works in conjunction with BTDedupStateData to manage the overall deduplication process
- The interval-based approach allows for efficient batch processing rather than individual tuple handling
- Used in both regular deduplication operations and WAL logging/replay for deduplication