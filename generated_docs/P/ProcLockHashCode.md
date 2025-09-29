# ProcLockHashCode

## Location
[src/backend/storage/lmgr/lock.c:552-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L552-L569)

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

## Simplified Source

```c
// Simplified version of ProcLockHashCode
static inline uint32
ProcLockHashCode(const PROCLOCKTAG *proclocktag, uint32 hashcode)
{
    // Start with the provided lock hash code
    uint32 lockhash = hashcode;

    // Get the process pointer as a Datum for hashing
    Datum procptr = PointerGetDatum(proclocktag->myProc);

    // XOR with shifted process pointer to create unique hash
    // The shift preserves lock partition alignment
    lockhash ^= ((uint32) procptr) << LOG2_NUM_LOCK_PARTITIONS;

    return lockhash;
}
```

Key simplifications made:
- Added descriptive comments explaining each step
- Simplified variable flow for clarity
- Explained the purpose of the XOR operation and bit shifting
- Maintained the essential hash computation algorithm
- Preserved the critical performance optimization aspect