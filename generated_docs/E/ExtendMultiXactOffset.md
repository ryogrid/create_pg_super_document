# ExtendMultiXactOffset

## Location
[src/backend/access/transam/multixact.c:2545-2576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2545-L2576)

## Overview
Ensures that the MultiXactOffset SLRU has sufficient space allocated for a newly-allocated MultiXactId by extending the offset control structure when needed.

## Definition
static void ExtendMultiXactOffset(MultiXactId multi)

## Detailed Description
This function is responsible for extending the MultiXactOffset SLRU (Simple LRU) buffer space when a new MultiXactId is allocated. It's designed to be very fast in the common case where no extension is needed. The function only performs work when the MultiXactId is the first entry of a new page, taking special care to handle wraparound scenarios where the first MultiXactId of page zero becomes FirstMultiXactId.

When extension is required, it acquires an exclusive lock on the appropriate SLRU bank, zeros out the new page, and creates an XLOG entry for crash recovery purposes. The function is called while holding MultiXactGenLock to ensure thread safety.

## Parameters / Member Variables
- `multi`: The MultiXactId for which offset space needs to be ensured

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactIdToOffsetEntry](../M/MultiXactIdToOffsetEntry.md)
  - [MultiXactIdToOffsetPage](../M/MultiXactIdToOffsetPage.md)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [ZeroMultiXactOffsetPage](../Z/ZeroMultiXactOffsetPage.md)
  - [LWLockRelease](../L/LWLockRelease.md)
- Called from (representative examples):
  - [GetNewMultiXactId](../G/GetNewMultiXactId.md)

## Notes and Other Information
- Static function, internal to multixact.c
- Called while holding MultiXactGenLock for thread safety
- Optimized for performance - does minimal work in most cases
- Only extends at the first MultiXactId of a new page
- Handles wraparound cases correctly with FirstMultiXactId
- Creates XLOG entries for crash recovery consistency
- Uses SLRU bank locking for fine-grained concurrency control