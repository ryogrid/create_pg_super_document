# SpeculativeInsertionLockAcquire

## Location
[src/backend/storage/lmgr/lmgr.c:778-803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L778-L803)

## Overview
Acquires a lock to indicate that a transaction is performing a speculative insertion but hasn't yet decided whether to commit or abort it.

## Definition


## Detailed Description
This function is part of PostgreSQL's speculative insertion mechanism, which is used primarily for handling unique constraint violations efficiently. When a transaction performs an insertion that might conflict with existing data (such as during ON CONFLICT handling), it first acquires a speculative insertion lock.

The function generates a unique token to distinguish multiple speculative insertions by the same transaction. It creates a lock tag specifically for speculative insertions and acquires an exclusive lock on it. Other transactions can wait on this lock to determine the outcome of the speculative insertion.

The token wrapping logic ensures that zero is never used as a token value, as zero typically represents "no token held" in the system.

## Parameters / Member Variables
- : The transaction ID that is performing the speculative insertion

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_SPECULATIVE_INSERTION
  - [LockAcquire](../L/LockAcquire.md)
  - ExclusiveLock
- Called from (representative examples):
  - [ExecInsert](../E/ExecInsert.md) (in nodeModifyTable.c:1115)

## Notes and Other Information
- Returns a uint32 token that uniquely identifies this speculative insertion within the transaction
- The global variable  is incremented to generate unique tokens
- Token values wrap around but skip zero to maintain the invariant that zero means no token
- Used in conjunction with SpeculativeInsertionLockRelease and SpeculativeInsertionWait for complete speculative insertion handling
- Essential for PostgreSQL's UPSERT (INSERT ... ON CONFLICT) functionality