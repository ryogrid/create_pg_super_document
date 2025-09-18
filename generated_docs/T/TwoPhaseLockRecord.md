# TwoPhaseLockRecord

## Location
src/backend/storage/lmgr/lock.c: 157 - 161

## Overview
A structure that records lock information when a lock is persisted to the 2PC state file during two-phase commit processing.

## Definition


## Detailed Description
TwoPhaseLockRecord is a simple data structure used in PostgreSQL's two-phase commit (2PC) implementation to store lock information that needs to be persisted across transaction boundaries. When a prepared transaction holds locks that must survive until the transaction is either committed or aborted, the details of those locks are serialized using this structure and written to the 2PC state file. This ensures that locks can be properly restored during recovery or when resuming a prepared transaction.

The structure is designed to be compact and contains only the essential information needed to recreate a lock: the lock tag that identifies what is being locked, and the lock mode that specifies the type of lock being held.

## Parameters / Member Variables
- : A LOCKTAG structure that uniquely identifies the object being locked, containing fields for database object identification and lock method
- : An integer value representing the type of lock being held (e.g., AccessShareLock, RowExclusiveLock, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG
  - LOCKMODE
- Called from (representative examples):
  - AtPrepare_Locks
  - lock_twophase_recover
  - lock_twophase_standby_recover
  - lock_twophase_postcommit

## Notes and Other Information
- This structure is specifically used in two-phase commit scenarios where transactions must be prepared and can be committed or aborted at a later time
- The record is written to persistent storage as part of the 2PC state file to ensure lock information survives system restarts
- During recovery operations, these records are read back to restore the proper lock state for prepared transactions
- The structure is kept minimal to reduce the overhead of persisting lock information to disk