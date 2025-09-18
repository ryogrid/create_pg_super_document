# TransactionIdFollows

## Location
src/backend/access/transam/transam.c: 314 - 328

## Overview
Determines whether one transaction ID logically follows (comes after) another, using PostgreSQL's modular transaction ID arithmetic.

## Definition
```c
bool TransactionIdFollows(TransactionId id1, TransactionId id2)
```

## Detailed Description
This function performs a logical comparison to determine if transaction ID `id1` follows (is greater than) transaction ID `id2` in PostgreSQL's transaction ordering system. It is the logical inverse of `TransactionIdPrecedes`. For permanent XIDs (special values), it uses simple unsigned integer comparison with the > operator. For normal transaction IDs, it performs modulo-2^32 arithmetic to handle transaction ID wraparound, returning true when the difference is greater than zero. This function is commonly used to determine if a transaction is more recent than another, which is crucial for conflict detection and transaction ordering.

## Parameters / Member Variables
- `id1`: The first transaction ID to compare
- `id2`: The second transaction ID to compare

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal
- Called from (representative examples):
  - HeapTupleHeaderAdvanceConflictHorizon
  - heap_prune_record_unchanged_lp_normal
  - SubTransSetParent
  - SnapBuildInitialSnapshot
  - GetConflictingVirtualXIDs
  - SerialAdd

## Notes and Other Information
- Provides the opposite comparison logic to `TransactionIdPrecedes` - returns true when `id1` is logically newer than `id2`
- Essential for conflict detection in serializable isolation level and logical replication
- Used in subtransaction management to ensure proper parent-child relationships
- The `> 0` comparison in the modulo arithmetic correctly identifies when `id1` follows `id2` in the circular transaction ID space
- Like other transaction ID comparison functions, assumes the compared IDs are not more than 2^31 transactions apart