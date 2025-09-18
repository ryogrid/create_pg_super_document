# TransactionIdPrecedes

## Location
src/backend/access/transam/transam.c: 280 - 298

## Overview
Determines whether one transaction ID logically precedes another, handling PostgreSQL's modular transaction ID arithmetic.

## Definition
```c
bool TransactionIdPrecedes(TransactionId id1, TransactionId id2)
```

## Detailed Description
This function performs a logical comparison to determine if transaction ID `id1` precedes transaction ID `id2` in PostgreSQL's transaction ordering system. The function handles both normal transaction IDs and permanent XIDs correctly. For permanent XIDs (special values like BootstrapTransactionId, FrozenTransactionId), it uses simple unsigned integer comparison. For normal transaction IDs, it performs modulo-2^32 arithmetic to handle transaction ID wraparound, which is a critical aspect of PostgreSQL's transaction management due to the finite 32-bit transaction ID space.

## Parameters / Member Variables
- `id1`: The first transaction ID to compare
- `id2`: The second transaction ID to compare

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal
- Called from (representative examples):
  - [heap_abort_speculative](../h/heap_abort_speculative.md)
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md)
  - [heap_prepare_freeze_tuple](../h/heap_prepare_freeze_tuple.md)
  - [HeapTupleSatisfiesVacuum](../H/HeapTupleSatisfiesVacuum.md)
  - [TransactionIdDidCommit](TransactionIdDidCommit.md)
  - TransactionIdIsInProgress
  - [xidLogicalComparator](../x/xidLogicalComparator.md)

## Notes and Other Information
- Critical for handling PostgreSQL's transaction ID wraparound problem - transaction IDs are 32-bit values that eventually wrap around
- The modulo-2^32 comparison ensures correct ordering even when transaction IDs wrap from the maximum value back to 0
- Widely used throughout the system for transaction visibility determinations, vacuum operations, and snapshot management
- The function assumes that the two transaction IDs being compared are not more than 2^31 transactions apart, which is enforced by PostgreSQL's transaction ID management policies