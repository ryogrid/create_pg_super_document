# MultiXactMember

## Location
src/include/access/multixact.h: 56 - 60

## Overview
MultiXactMember is a structure that represents a single transaction member within a PostgreSQL multi-transaction, containing both the transaction ID and its locking status.

## Definition


## Detailed Description
MultiXactMember is a fundamental data structure in PostgreSQL's multi-transaction system used to store information about individual transactions that participate in a multi-transaction. Multi-transactions allow multiple concurrent transactions to hold different types of locks on the same tuple simultaneously. Each MultiXactMember represents one transaction's participation in such a multi-transaction, tracking both the transaction identifier and the specific type of lock it holds.

The structure is used extensively throughout the heap access methods and multi-transaction management code to represent, manipulate, and track the various transactions that make up a multi-transaction ID.

## Parameters / Member Variables
- : The transaction ID (TransactionId) of the participating transaction
- : The lock mode/status (MultiXactStatus) that this transaction holds, which can be one of:
  - MultiXactStatusForKeyShare (0x00): FOR KEY SHARE lock
  - MultiXactStatusForShare (0x01): FOR SHARE lock  
  - MultiXactStatusForNoKeyUpdate (0x02): FOR NO KEY UPDATE lock
  - MultiXactStatusForUpdate (0x03): FOR UPDATE lock
  - MultiXactStatusNoKeyUpdate (0x04): Update that doesn't touch key columns
  - MultiXactStatusUpdate (0x05): Other updates and delete operations

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (from transaction system)
  - [MultiXactStatus](MultiXactStatus.md) (enumeration defined in same header)
- Called from (representative examples):
  - [heap_lock_tuple](../h/heap_lock_tuple.md): Uses MultiXactMember for tuple locking operations
  - [MultiXactIdCreate](MultiXactIdCreate.md): Creates multi-transactions from MultiXactMember arrays
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md): Retrieves MultiXactMember arrays from stored multi-transactions
  - [xl_multixact_create](../x/xl_multixact_create.md): WAL record structure that contains MultiXactMember arrays

## Notes and Other Information
- This structure is part of PostgreSQL's sophisticated concurrency control mechanism that allows fine-grained locking
- [MultiXactMember](MultiXactMember.md) arrays are stored both in memory (for active multi-transactions) and on disk (in the pg_multixact directory)
- The structure is designed to be compact and efficient as it may be stored in large arrays
- Used extensively in heap tuple operations, especially for row-level locking scenarios
- Critical for implementing PostgreSQL's MVCC (Multi-Version Concurrency Control) system with multiple lock types