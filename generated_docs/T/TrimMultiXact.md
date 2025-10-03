# TrimMultiXact

## Location
[src/backend/access/transam/multixact.c:2170-2273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2170-L2273)

## Overview
Performs final cleanup and initialization of MultiXact data structures that must be called ONCE at the end of startup/recovery.

## Definition

```c
void
TrimMultiXact(void)
```
## Detailed Description
TrimMultiXact is responsible for finalizing the MultiXact subsystem after database startup or recovery. It performs critical cleanup operations to ensure the MultiXact system is in a consistent state for normal operation. The function operates in several phases:

1. **State Capture**: Acquires the current MultiXact state including the next MultiXact ID, offset, and oldest MultiXact information under MultiXactGenLock protection.

2. **Offsets Cleanup**: Reinitializes the latest page number for offsets and zeros out the remainder of the current offsets page to prevent obsolete data from interfering with normal operations. This is particularly important because MultiXact ignores the WAL rule "write xlog before data," so successor entries may contain obsolete nonzero offset values.

3. **Members Cleanup**: Similarly reinitializes the latest page number for members and zeros out the remainder of the current members page to ensure clean state.

4. **Finalization**: Marks the MultiXact subsystem as officially up and running by setting the finishedStartup flag, then computes the next wraparound limit.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)  
  - [MultiXactIdToOffsetPage](../M/MultiXactIdToOffsetPage.md)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [MultiXactIdToOffsetEntry](../M/MultiXactIdToOffsetEntry.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [SimpleLruReadPage](../S/SimpleLruReadPage.md)
  - MemSet
  - [MXOffsetToMemberPage](../M/MXOffsetToMemberPage.md)
  - [MXOffsetToFlagsOffset](../M/MXOffsetToFlagsOffset.md)
  - [MXOffsetToMemberOffset](../M/MXOffsetToMemberOffset.md)
  - [SetMultiXactIdLimit](../S/SetMultiXactIdLimit.md)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md)

## Notes and Other Information
- Must be called exactly ONCE at the end of startup/recovery
- Uses shared memory structures MultiXactOffsetCtl and MultiXactMemberCtl
- Critical for preventing data corruption in MultiXact operations
- The zeroing operations are necessary because MultiXact doesn't follow strict WAL ordering rules
- Sets finishedStartup flag to indicate the subsystem is ready for normal operations
- Computes wraparound limits as final step to ensure proper MultiXact ID management

## Simplified Source

```c
// Simplified version of TrimMultiXact
void TrimMultiXact(void) {
    MultiXactId nextMXact;
    MultiXactOffset offset;
    MultiXactId oldestMXact;
    Oid oldestMXactDB;
    int64 pageno;
    int entryno;
    int flagsoff;

    // Step 1: Capture current MultiXact state under lock protection
    LWLockAcquire(MultiXactGenLock, LW_SHARED);
    nextMXact = MultiXactState->nextMXact;
    offset = MultiXactState->nextOffset;
    oldestMXact = MultiXactState->oldestMultiXactId;
    oldestMXactDB = MultiXactState->oldestMultiXactDB;
    LWLockRelease(MultiXactGenLock);

    // Step 2: Clean up offsets - initialize latest page and zero remainder
    pageno = MultiXactIdToOffsetPage(nextMXact);
    pg_atomic_write_u64(&MultiXactOffsetCtl->shared->latest_page_number, pageno);

    entryno = MultiXactIdToOffsetEntry(nextMXact);
    if (entryno != 0) {
        // Zero out remainder of current offsets page to prevent obsolete data
        LWLock *lock = SimpleLruGetBankLock(MultiXactOffsetCtl, pageno);
        LWLockAcquire(lock, LW_EXCLUSIVE);

        int slotno = SimpleLruReadPage(MultiXactOffsetCtl, pageno, true, nextMXact);
        MultiXactOffset *offptr = (MultiXactOffset *)
            MultiXactOffsetCtl->shared->page_buffer[slotno];
        offptr += entryno;

        MemSet(offptr, 0, BLCKSZ - (entryno * sizeof(MultiXactOffset)));
        MultiXactOffsetCtl->shared->page_dirty[slotno] = true;
        LWLockRelease(lock);
    }

    // Step 3: Clean up members - initialize latest page and zero remainder
    pageno = MXOffsetToMemberPage(offset);
    pg_atomic_write_u64(&MultiXactMemberCtl->shared->latest_page_number, pageno);

    flagsoff = MXOffsetToFlagsOffset(offset);
    if (flagsoff != 0) {
        // Zero out remainder of current members page
        LWLock *lock = SimpleLruGetBankLock(MultiXactMemberCtl, pageno);
        LWLockAcquire(lock, LW_EXCLUSIVE);

        int memberoff = MXOffsetToMemberOffset(offset);
        int slotno = SimpleLruReadPage(MultiXactMemberCtl, pageno, true, offset);
        TransactionId *xidptr = (TransactionId *)
            (MultiXactMemberCtl->shared->page_buffer[slotno] + memberoff);

        MemSet(xidptr, 0, BLCKSZ - memberoff);
        MultiXactMemberCtl->shared->page_dirty[slotno] = true;
        LWLockRelease(lock);
    }

    // Step 4: Mark subsystem as officially ready
    LWLockAcquire(MultiXactGenLock, LW_EXCLUSIVE);
    MultiXactState->finishedStartup = true;
    LWLockRelease(MultiXactGenLock);

    // Step 5: Compute next wraparound limit
    SetMultiXactIdLimit(oldestMXact, oldestMXactDB, true);
}
```

Key simplifications made:
- Consolidated variable declarations at the top for clarity
- Added step-by-step comments explaining the main phases
- Removed detailed comments about WAL ordering rules (kept essential logic)
- Simplified complex pointer arithmetic explanations
- Consolidated similar cleanup patterns for offsets and members
- Maintained all critical functionality and error handling
- Preserved exact lock acquisition/release patterns for correctness