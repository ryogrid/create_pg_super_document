# TransactionIdPrecedesOrEquals

## Location
src/backend/access/transam/transam.c: 299 - 313

## Overview
Determines whether one transaction ID logically precedes or equals another, using PostgreSQL's modular transaction ID arithmetic.

## Definition
```c
bool TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2)
```

## Detailed Description
This function performs a logical comparison to determine if transaction ID `id1` precedes or equals transaction ID `id2` in PostgreSQL's transaction ordering system. Similar to `TransactionIdPrecedes`, it handles both normal transaction IDs and permanent XIDs correctly. For permanent XIDs, it uses simple unsigned integer comparison with the <= operator. For normal transaction IDs, it performs modulo-2^32 arithmetic to handle transaction ID wraparound, returning true when the difference is less than or equal to zero. This function is essential for range checks and boundary conditions in transaction visibility and snapshot management.

## Parameters / Member Variables
- `id1`: The first transaction ID to compare
- `id2`: The second transaction ID to compare

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal
- Called from (representative examples):
  - [heap_prepare_freeze_tuple](../h/heap_prepare_freeze_tuple.md)
  - [heap_tuple_should_freeze](../h/heap_tuple_should_freeze.md)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md)
  - SnapBuildFindSnapshot
  - TransactionIdIsInProgress
  - ComputeXidHorizons

## Notes and Other Information
- Extends the functionality of `TransactionIdPrecedes` by including equality in the comparison
- Critical for transaction visibility determinations where both preceding and equal transaction IDs are considered valid
- Used extensively in snapshot management, vacuum operations, and replication slot management
- The `<= 0` comparison in the modulo arithmetic correctly handles both preceding and equal transaction IDs
- Like `TransactionIdPrecedes`, assumes transaction IDs being compared are not more than 2^31 transactions apart