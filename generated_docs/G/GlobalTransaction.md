# GlobalTransaction

## Location
[src/include/access/twophase.h:26-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/twophase.h#L26-L65)

## Overview
GlobalTransaction is a typedef for a pointer to GlobalTransactionData, representing a global transaction that is in prepared state or attempting to become prepared in PostgreSQL's two-phase commit protocol.

## Definition

```c
typedef struct GlobalTransactionData *GlobalTransaction;
```
## Detailed Description
GlobalTransaction serves as an opaque handle to the internal GlobalTransactionData structure in PostgreSQL's two-phase commit implementation. It provides a clean interface for external code to reference global transactions without exposing the internal structure details.

The actual GlobalTransactionData structure tracks the complete lifecycle of a prepared transaction, from initial preparation through final commit or rollback. The typedef design pattern maintains abstraction boundaries, with the full structure definition residing in twophase.c while the header file only exposes the pointer type.

This design enables PostgreSQL to manage prepared transactions across system boundaries, supporting distributed transaction scenarios where a transaction coordinator needs to ensure atomicity across multiple database instances.

## Parameters / Member Variables
Since this is a typedef for a pointer, the actual members are in GlobalTransactionData:
- : Pointer to next GlobalTransaction in free list
- : ID of associated dummy PGPROC entry
- : Timestamp when transaction was prepared
- : WAL LSN where prepare record starts
- : WAL LSN where prepare record ends
- : Transaction ID of the global transaction
- : User ID that executed the transaction
- : Backend currently working on the transaction
- : True if PGPROC entry is in process array
- : True if prepare state file is written to disk
- : True if entry was added during WAL recovery
- : Global Identifier string for the prepared transaction

## Dependencies
- Functions that operate on GlobalTransaction:
  - MarkAsPreparing: Creates and initializes a global transaction entry
  - StartPrepare: Begins the prepare phase
  - EndPrepare: Completes the prepare phase
  - FinishPreparedTransaction: Commits or rolls back prepared transaction
  - LockGXact: Locks a global transaction for exclusive access
  - RemoveGXact: Removes global transaction from memory
- Used extensively throughout:
  - Two-phase commit protocol implementation
  - WAL recovery procedures
  - Transaction state management
  - Prepared transaction queries and views

## Notes and Other Information
- The opaque pointer design ensures internal structure changes don't break external interfaces
- Global transactions persist across PostgreSQL restarts via WAL logging
- Each global transaction consumes a slot in the max_prepared_xacts-sized array
- The typedef pattern follows PostgreSQL's convention for complex internal structures
- Critical for supporting XA transactions and distributed database scenarios
- Memory management handled entirely within twophase.c module