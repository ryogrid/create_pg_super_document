# TrimMultiXact

## Location
src/backend/access/transam/multixact.c: 2170 - 2273

## Overview
Performs final cleanup and initialization of MultiXact data structures that must be called ONCE at the end of startup/recovery.

## Definition


## Detailed Description
TrimMultiXact is responsible for finalizing the MultiXact subsystem after database startup or recovery. It performs critical cleanup operations to ensure the MultiXact system is in a consistent state for normal operation. The function operates in several phases:

1. **State Capture**: Acquires the current MultiXact state including the next MultiXact ID, offset, and oldest MultiXact information under MultiXactGenLock protection.

2. **Offsets Cleanup**: Reinitializes the latest page number for offsets and zeros out the remainder of the current offsets page to prevent obsolete data from interfering with normal operations. This is particularly important because MultiXact ignores the WAL rule "write xlog before data," so successor entries may contain obsolete nonzero offset values.

3. **Members Cleanup**: Similarly reinitializes the latest page number for members and zeros out the remainder of the current members page to ensure clean state.

4. **Finalization**: Marks the MultiXact subsystem as officially up and running by setting the finishedStartup flag, then computes the next wraparound limit.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease  
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