# ProcLockHashCode

## Location
src/backend/storage/lmgr/lock.c: 552 - 569

## Overview
An optimized inline hash function that computes the hash code for a PROCLOCKTAG when the underlying LOCK's hash code is already known, avoiding redundant hash calculations.

## Definition
static inline uint32 ProcLockHashCode(const PROCLOCKTAG *proclocktag, uint32 hashcode)

## Detailed Description
ProcLockHashCode is a performance optimization that computes the same hash value as proclock_hash() but takes the LOCK's hash code as a parameter instead of recomputing it. This function is used when the caller already has the LOCK hash code available, avoiding the overhead of calling LockTagHashCode() again.

The function applies the same XOR operation with the left-shifted PGPROC address to ensure the hash depends on both the lock and the specific process, maintaining the same partition alignment guarantees as proclock_hash().

## Parameters / Member Variables
- proclocktag: Pointer to the PROCLOCKTAG structure containing the process and lock information
- hashcode: Pre-computed hash code for the underlying LOCK object

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](PointerGetDatum.md): Converts the PGPROC pointer to Datum for hash computation
  - LOG2_NUM_LOCK_PARTITIONS: Used to left-shift the process pointer to preserve partition bits
- Called from (representative examples):
  - [LockAcquireExtended](../L/LockAcquireExtended.md): During lock acquisition when LOCK hash is already computed
  - [SetupLockInTable](../S/SetupLockInTable.md): When setting up PROCLOCK entries in the hash table
  - [CleanUpLock](../C/CleanUpLock.md): During lock cleanup operations
  - [FastPathGetRelationLockEntry](../F/FastPathGetRelationLockEntry.md): In fast-path lock operations
  - [LockRefindAndRelease](../L/LockRefindAndRelease.md): When refinding and releasing locks
  - [lock_twophase_recover](../l/lock_twophase_recover.md): During two-phase commit recovery

## Notes and Other Information
- This is an inline function for performance optimization
- Must produce identical results to proclock_hash() - the comment emphasizes this requirement
- Used extensively throughout the lock manager when LOCK hash codes are already available
- Critical for maintaining consistent hash partitioning between LOCK and PROCLOCK tables
- The inline nature reduces function call overhead during frequent lock operations