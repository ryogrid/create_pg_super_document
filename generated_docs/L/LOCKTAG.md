# LOCKTAG

## Location
src/include/storage/lock.h: 164 - 172

## Overview
LOCKTAG is the key data structure used to uniquely identify lockable objects in PostgreSQL's lock manager. It serves as the hash key for looking up locks in the shared lock hashtable.

## Definition


Related enum:


## Detailed Description
LOCKTAG is carefully designed to be exactly 16 bytes with no padding, making it efficient for use as a hash key. The structure provides a generic framework for identifying different types of lockable resources in PostgreSQL.

The four field slots (field1-field4) are used differently depending on the lock type. For example:
- For relation locks: field1=database OID, field2=relation OID
- For page locks: field1=database OID, field2=relation OID, field3=block number
- For tuple locks: field1=database OID, field2=relation OID, field3=block number, field4=tuple offset

The design allows a single hash table in shared memory to store locks of different lock methods by including the lockmethodid in the tag. This unified approach simplifies the lock manager architecture while maintaining type safety through the locktag_type field.

## Parameters / Member Variables
- : First 32-bit identifier field (interpretation depends on lock type)
- : Second 32-bit identifier field (interpretation depends on lock type) 
- : Third 32-bit identifier field (interpretation depends on lock type)
- : Fourth 16-bit identifier field (interpretation depends on lock type)
- : Type of lock from LockTagType enum, determines how to interpret the field values
- : Identifies which lock method this tag belongs to (DEFAULT_LOCKMETHOD or USER_LOCKMETHOD)

## Dependencies
- Functions called/Symbols referenced:
  - LockTagType (enum)
  - LOCKMETHODID
- Called from (representative examples):
  - LockAcquire
  - LockRelease
  - SetupLockInTable
  - LockHeldByMe
  - GetLockConflicts
  - LockTagHashCode

## Notes and Other Information
- Deliberately sized to exactly 16 bytes with no padding for efficient hashing and memory usage
- The structure would need adjustment if Oid, BlockNumber, or TransactionId were widened beyond 32 bits
- Different lock types interpret the field1-field4 values differently based on locktag_type
- Used as the key in PostgreSQL's shared lock hash table
- The unified design allows different lock methods to coexist in the same hash table
- Critical for the performance of lock operations as it's used in hash computations
- The field layout is optimized for fast equality comparisons and hash code generation
- Support for up to 256 different LockTagType values and 65536 lock methods