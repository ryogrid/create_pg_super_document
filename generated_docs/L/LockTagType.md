# LockTagType

## Location
[src/include/storage/lock.h:150-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L150-L151)

## Overview
LockTagType is an enumeration that defines the different kinds of objects that can be locked in PostgreSQL's lock management system. It serves as a key component in the LOCKTAG structure to uniquely identify lockable objects.

## Definition


## Detailed Description
The LockTagType enumeration is a fundamental part of PostgreSQL's lock management system. It categorizes the different types of database objects and resources that can be locked to ensure concurrency control and data consistency. Each lock tag type represents a different granularity or category of lockable resource, from entire relations down to individual tuples.

This enumeration is used as part of the LOCKTAG structure, which serves as the key for looking up LOCK items in the lock hashtable. The design allows PostgreSQL to handle up to 256 different lock tag types, providing flexibility for future extensions.

The lock tag types cover various levels of granularity:
- Database-level locks (relations, objects)
- Page-level locks for fine-grained control
- Tuple-level locks for maximum concurrency
- Transaction-related locks for coordination
- Special-purpose locks for specific operations

## Parameters / Member Variables
- : Locks an entire relation (table, index, etc.)
- : Controls the right to extend a relation by adding new pages
- : Protects the pg_database.datfrozenxid field during updates
- : Locks a single page within a relation
- : Locks an individual tuple (row) for fine-grained concurrency
- : Used for waiting on regular transaction completion
- : Used for waiting on virtual transaction completion
- : Manages speculative insertion tokens and XIDs
- : Locks non-relation database objects (functions, types, etc.)
- : Reserved for legacy contrib/userlock extension compatibility
- : Provides user-defined advisory locks for application coordination
- : Manages transactions being applied in logical replication

## Dependencies
- Functions called/Symbols referenced: None directly (enum definition)
- Called from (representative examples):
  - DescribeLockTag
  - [pg_lock_status](../p/pg_lock_status.md)
  - LOCK_LOCKTAG macro
  - LOCALLOCK_LOCKTAG macro

## Notes and Other Information
- The enumeration is accompanied by  array that provides human-readable string names for each lock type
-  is defined as  to mark the end of the enumeration
- The system supports up to 256 different lock tag types, though currently only 12 are defined
- Each lock tag type corresponds to a specific locking strategy and granularity level in PostgreSQL's MVCC system
- Advisory locks () allow applications to coordinate operations using PostgreSQL's lock infrastructure
- The lock tag type is a critical component in PostgreSQL's deadlock detection and resolution algorithms