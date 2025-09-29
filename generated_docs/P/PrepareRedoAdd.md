# PrepareRedoAdd

## Location
[src/backend/access/transam/twophase.c:2470-2571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L2470-L2571)

## Overview
PrepareRedoAdd creates and registers a global transaction entry in shared memory during WAL recovery, tracking prepared transaction state from either WAL records or existing disk files.

## Definition

```c
struct and puts it into the active array.
	 *
	 * In redo, this struct is mainly used to track PREPARE/COMMIT entries in
	 * shared memory. Hence, we only fill up the bare minimum contents here.
	 * The gxact also gets marked with gxact->inredo set to true to indicate
	 * that it got added in the redo phase
	 */

	/*
	 * In the event of a crash while a checkpoint was running, it may be
	 * possible that some two-phase data found its way to disk while its
	 * corresponding record needs to be replayed in the follow-up recovery. As
	 * the 2PC data was on disk, it has already been restored at the beginning
	 * of recovery with restoreTwoPhaseData(), so skip this record to avoid
	 * duplicates in TwoPhaseState.  If a consistent state has been reached,
	 * the record is added to TwoPhaseState and it should have no
	 * corresponding file in pg_twophase.
	 */
	if (!XLogRecPtrIsInvalid(start_lsn))
	{
		char		path[MAXPGPATH];

		TwoPhaseFilePath(path, hdr->xid);

		if (access(path, F_OK) == 0)
		{
			ereport(reachedConsistency ? ERROR : WARNING,
					(errmsg("could not recover two-phase state file for transaction %u",
							hdr->xid),
					 errdetail("Two-phase state file has been found in WAL record %X/%X, but this transaction has already been restored from disk.",
							   LSN_FORMAT_ARGS(start_lsn))));
			return;
		}

		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not access file \"%s\": %m", path)));
	}

	/* Get a free gxact from the freelist */
	if (TwoPhaseState->freeGXacts == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("maximum number of prepared transactions reached"),
				 errhint("Increase \"max_prepared_transactions\" (currently %d).",
						 max_prepared_xacts)));
```
## Detailed Description
PrepareRedoAdd is a critical function in PostgreSQL's two-phase commit recovery process that creates global transaction entries in the TwoPhaseState shared memory structure during WAL replay. The function handles both scenarios where two-phase data is available in WAL records (start_lsn is valid) and where data has already been restored from disk files (start_lsn is invalid). It includes sophisticated duplicate detection logic to handle crash scenarios where some two-phase data may exist both in WAL records and on disk. The function also manages replication origin advancement for logical replication scenarios and maintains proper state tracking with the inredo flag.

## Parameters / Member Variables
- `path[MAXPGPATH]`: Buffer containing the two-phase transaction data including header and global transaction identifier
- `hdr->xid)`: WAL log sequence number where the prepare record starts (InvalidXLogRecPtr if reading from disk)
- `LSN_FORMAT_ARGS(start_lsn))))`: WAL log sequence number where the prepare record ends
- `return`: Replication origin identifier for logical replication tracking (InvalidRepOriginId if not applicable)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - XLogRecPtrIsInvalid
  - [TwoPhaseFilePath](../T/TwoPhaseFilePath.md)
  - access
  - [replorigin_advance](../r/replorigin_advance.md)
- Called from (representative examples):
  - [restoreTwoPhaseData](../r/restoreTwoPhaseData.md)
  - [xact_redo](../x/xact_redo.md)

## Notes and Other Information
The function requires exclusive access to TwoPhaseStateLock and can only be called during recovery. It performs careful duplicate detection by checking for existing files when processing WAL records, preventing corruption during crash recovery scenarios. The function allocates global transaction structures from the free list and properly initializes all necessary fields including prepare timestamps, LSN positions, and state flags. Error handling distinguishes between consistency-reached and pre-consistency states. Location: src/backend/access/transam/twophase.c:2470-2571

## Simplified Source

```c
// Simplified version of PrepareRedoAdd
void PrepareRedoAdd(char *buf, XLogRecPtr start_lsn,
                   XLogRecPtr end_lsn, RepOriginId origin_id)
{
    TwoPhaseFileHeader *hdr = (TwoPhaseFileHeader *) buf;
    char *bufptr = buf + MAXALIGN(sizeof(TwoPhaseFileHeader));
    const char *gid = (const char *) bufptr;
    GlobalTransaction gxact;

    // Core logic step 1: Check for duplicate 2PC data on disk
    if (!XLogRecPtrIsInvalid(start_lsn)) {
        char path[MAXPGPATH];
        TwoPhaseFilePath(path, hdr->xid);

        // Skip if file already exists on disk to avoid duplicates
        if (access(path, F_OK) == 0) {
            ereport(reachedConsistency ? ERROR : WARNING,
                    (errmsg("transaction %u already restored from disk", hdr->xid)));
            return;
        }
    }

    // Core logic step 2: Allocate new global transaction entry
    if (TwoPhaseState->freeGXacts == NULL) {
        ereport(ERROR, (errmsg("maximum prepared transactions reached")));
    }
    gxact = TwoPhaseState->freeGXacts;
    TwoPhaseState->freeGXacts = gxact->next;

    // Core logic step 3: Initialize transaction state
    gxact->prepared_at = hdr->prepared_at;
    gxact->prepare_start_lsn = start_lsn;
    gxact->prepare_end_lsn = end_lsn;
    gxact->xid = hdr->xid;
    gxact->owner = hdr->owner;
    gxact->valid = false;
    gxact->ondisk = XLogRecPtrIsInvalid(start_lsn);
    gxact->inredo = true;  // Mark as added during redo
    strcpy(gxact->gid, gid);

    // Core logic step 4: Add to active transaction array
    TwoPhaseState->prepXacts[TwoPhaseState->numPrepXacts++] = gxact;

    // Core logic step 5: Handle replication origin advancement
    if (origin_id != InvalidRepOriginId) {
        replorigin_advance(origin_id, hdr->origin_lsn, end_lsn, false, false);
    }
}
```

Key simplifications made:
- Removed detailed assertion checks for clarity
- Consolidated error handling into essential checks only
- Simplified file access error handling (removed errno checking)
- Abstracted complex error message formatting
- Removed debug logging statement
- Streamlined variable initialization
- Added clear step-by-step comments explaining the core algorithm
- Maintained all essential functionality while reducing code by ~40%