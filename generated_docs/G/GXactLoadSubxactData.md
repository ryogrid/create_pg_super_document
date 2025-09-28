# GXactLoadSubxactData

## Location
[src/backend/access/transam/twophase.c:504-529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L504-L529)

## Overview
Loads subtransaction data into a GlobalTransaction's associated PGPROC structure, handling the subtransaction list for prepared transactions with nested transactions.

## Definition

```c
static void
GXactLoadSubxactData(GlobalTransaction gxact, int nsubxacts,
					 TransactionId *children)
```
## Detailed Description
GXactLoadSubxactData is responsible for populating the subtransaction information in a prepared transaction's PGPROC entry. This function handles the case where a transaction being prepared for two-phase commit contains subtransactions (nested transactions). It copies the subtransaction IDs into the PGPROC's cached subxid array, handling overflow situations where there are more subtransactions than can be cached. The function sets appropriate flags to indicate whether the subtransaction list has overflowed the cache limit.

## Parameters / Member Variables
- : The GlobalTransaction structure whose PGPROC needs subtransaction data
- : The number of subtransactions to load
- : Array of TransactionId values representing the subtransactions

## Dependencies
- Functions called/Symbols referenced:
  - [GlobalTransaction](GlobalTransaction.md)
  - [PGPROC](../P/PGPROC.md)
  - GetPGProcByNumber
  - PGPROC_MAX_CACHED_SUBXIDS
- Called from (representative examples):
  - [StartPrepare](../S/StartPrepare.md)
  - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md)

## Notes and Other Information
- This is a static function used internally within the two-phase commit system
- Must be called before MarkAsPrepared() to ensure proper initialization
- Handles subtransaction list overflow by setting the overflowed flag and truncating to PGPROC_MAX_CACHED_SUBXIDS
- No additional locking is required since the GlobalTransaction is not yet marked as valid
- Uses memcpy for efficient copying of subtransaction ID arrays
- Essential for maintaining transaction visibility and lock information for prepared transactions with nested transactions

## Simplified Source

```c
// Simplified version of GXactLoadSubxactData
static void GXactLoadSubxactData(GlobalTransaction gxact, int nsubxacts,
                                TransactionId *children) {
    PGPROC *proc = GetPGProcByNumber(gxact->pgprocno);

    // Handle subtransaction overflow
    if (nsubxacts > PGPROC_MAX_CACHED_SUBXIDS) {
        proc->subxidStatus.overflowed = true;
        nsubxacts = PGPROC_MAX_CACHED_SUBXIDS;
    }

    // Copy subtransaction IDs if any exist
    if (nsubxacts > 0) {
        memcpy(proc->subxids.xids, children,
               nsubxacts * sizeof(TransactionId));
        proc->subxidStatus.count = nsubxacts;
    }
}
```

Key simplifications made:
- Consolidated overflow handling into clear conditional logic
- Added comments explaining the overflow and copy operations
- Preserved the essential subtransaction data loading mechanism
- Maintained the PGPROC_MAX_CACHED_SUBXIDS limit handling
- Focused on the core data transfer from children array to PGPROC