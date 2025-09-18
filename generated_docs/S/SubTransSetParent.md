# SubTransSetParent

## Location
src/backend/access/transam/subtrans.c: 85 - 121

## Overview
Records the parent transaction ID of a subtransaction in the subtransaction log, establishing the parent-child relationship hierarchy for nested transactions.

## Definition
```c
void
SubTransSetParent(TransactionId xid, TransactionId parent)
```

## Detailed Description
SubTransSetParent stores the parent-child relationship between a subtransaction and its parent transaction in the SUBTRANS log. This function is essential for PostgreSQL's nested transaction support, allowing the system to track the hierarchical structure of subtransactions. The function uses the Simple LRU buffer management system to efficiently handle page-based storage of subtransaction relationships.

The function performs validation to ensure the parent transaction ID is valid and that the child transaction follows the parent (using transaction ID ordering). It handles concurrent access through LWLock exclusive locking and ensures data integrity by preventing overwrites of existing valid parent relationships.

## Parameters / Member Variables
- `xid`: The subtransaction ID whose parent is being recorded
- `parent`: The parent transaction ID to be associated with the subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdToPage (converts transaction ID to page number)
  - TransactionIdToEntry (converts transaction ID to entry within page)
  - TransactionIdFollows (validates transaction ID ordering)
  - SimpleLruGetBankLock (obtains lock for SLRU page)
  - SimpleLruReadPage (reads/loads the appropriate SLRU page)
  - SubTransCtl (global SLRU control structure for subtransactions)
  - LWLock (lightweight lock type)
- Called from (representative examples):
  - ProcessTwoPhaseBuffer (during two-phase commit recovery)
  - AssignTransactionId (when assigning transaction IDs)
  - ProcArrayApplyXidAssignment (during transaction ID assignment in standby)

## Notes and Other Information
- Uses exclusive LWLock to ensure thread-safe access to subtransaction data
- Validates that subtransaction follows its parent using transaction ID arithmetic
- Prevents corruption by asserting that existing entries are either invalid or match the new parent
- Part of PostgreSQL's MVCC implementation for handling nested transactions
- The function can be called multiple times for the same subtransaction but should always set the same parent
- Uses Simple LRU (SLRU) buffer management for efficient page-based storage