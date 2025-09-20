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
  - LWLockHeldByMeInMode
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