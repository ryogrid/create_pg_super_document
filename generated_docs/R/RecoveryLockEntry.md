# RecoveryLockEntry

## Location
[src/backend/storage/ipc/standby.c:52-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L52-L56)

## Overview
A structure used to track exclusive locks owned by original transactions during standby recovery, stored in a hash table for efficient lookup and chained together per transaction.

## Definition

```c
typedef struct RecoveryLockEntry
{
	xl_standby_lock key;		/* hash key: xid, dbOid, relOid */
	struct RecoveryLockEntry *next; /* chain link */
} RecoveryLockEntry;
```
## Detailed Description
 is a data structure used in PostgreSQL's standby recovery system to keep track of all exclusive locks owned by original transactions. Each entry represents a single exclusive lock and is stored in the  hash table. The structure is designed to efficiently manage lock information during recovery by using both hash table lookup (via the key) and linked list chaining (via the next pointer) to group all locks belonging to the same transaction together.

The recovery system uses this structure to maintain consistency during standby recovery by tracking which locks were held by transactions in the primary database. This information is essential for ensuring that the standby server can properly handle conflicts and maintain data consistency during recovery operations.

## Parameters / Member Variables
- `key`: An  structure containing the hash key composed of transaction ID (xid), database OID (dbOid), and relation OID (relOid) that uniquely identifies the exclusive lock
- `*next`: A pointer to the next  in the chain, used to link all lock entries belonging to the same transaction together
## Dependencies
- Functions called/Symbols referenced:
  -  (used as key field type)
- Called from (representative examples):
  - 
  - 
  - 

## Notes and Other Information
- Each  corresponds to one exclusive lock held by an original transaction
- All entries for a given transaction are chained together using the  pointer for efficient traversal
- The structure works in conjunction with  which maintains the head of the chain for each transaction
- Part of PostgreSQL's standby recovery infrastructure located in 
- The hash key enables O(1) lookup of specific locks by transaction ID, database, and relation