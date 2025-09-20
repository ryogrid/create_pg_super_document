# RecoveryLockXidEntry

## Location
[src/backend/storage/ipc/standby.c:58-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L58-L62)

## Overview
A structure that serves as the entry point for tracking all exclusive locks belonging to a specific transaction during standby recovery, storing the transaction ID and head of the lock chain.

## Definition

```c
typedef struct RecoveryLockXidEntry
{
	TransactionId xid;			/* hash key -- must be first */
	struct RecoveryLockEntry *head; /* chain head */
} RecoveryLockXidEntry;
```
## Detailed Description
 is a data structure used in PostgreSQL's standby recovery system to organize and manage all exclusive locks owned by a single original transaction. This structure acts as the entry point in the  hash table, where each entry corresponds to one transaction that holds exclusive locks. The structure maintains a pointer to the head of a linked list of  structures, allowing efficient access to all locks belonging to a specific transaction.

This design provides a two-level organization: the hash table provides O(1) access to a transaction's lock information, while the linked list allows iteration through all locks owned by that transaction. This is particularly useful during recovery operations when locks need to be released or when checking for lock conflicts.

## Parameters / Member Variables
- `xid`: A  that serves as the hash key for the  table. The comment indicates it must be the first field, likely for hash table implementation requirements
- `*head`: A pointer to the first  in the chain of all exclusive locks owned by this transaction
## Dependencies
- Functions called/Symbols referenced:
  -  (used as target type for head pointer)
  -  (used as xid field type)
- Called from (representative examples):
  - 
  - 
  - 
  - 
  - 
  - 

## Notes and Other Information
- The  field must be the first member of the structure, as indicated by the comment, which is a common requirement for hash table implementations
- Each  represents one transaction and points to all its exclusive locks via the  pointer
- Works in conjunction with  to provide a complete lock tracking system during recovery
- Part of PostgreSQL's standby recovery infrastructure located in 
- The structure enables efficient lock management operations such as releasing all locks for a specific transaction or checking transaction-specific lock conflicts
- Used by multiple standby lock management functions for various recovery scenarios including releasing locks by transaction, releasing all locks, and releasing old locks