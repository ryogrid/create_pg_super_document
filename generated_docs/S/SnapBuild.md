# SnapBuild

## Location
[src/backend/replication/logical/snapbuild.c:152-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L152-L323)

## Overview
SnapBuild is a core data structure in PostgreSQL's logical replication system that manages the state and process of building consistent snapshots for logical decoding, tracking transaction visibility and catalog changes during WAL replay.

## Definition

```c
struct SnapBuild
{
	/* how far are we along building our first full snapshot */
	SnapBuildState state;

	/* private memory context used to allocate memory for this module. */
	MemoryContext context;

	/* all transactions < than this have committed/aborted */
	TransactionId xmin;

	/* all transactions >= than this are uncommitted */
	TransactionId xmax;

	/*
	 * Don't replay commits from an LSN < this LSN. This can be set externally
	 * but it will also be advanced (never retreat) from within snapbuild.c.
	 */
	XLogRecPtr	start_decoding_at;

	/*
	 * LSN at which two-phase decoding was enabled or LSN at which we found a
	 * consistent point at the time of slot creation.
	 *
	 * The prepared transactions, that were skipped because previously
	 * two-phase was not enabled or are not covered by initial snapshot, need
	 * to be sent later along with commit prepared and they must be before
	 * this point.
	 */
	XLogRecPtr	two_phase_at;

	/*
	 * Don't start decoding WAL until the "xl_running_xacts" information
	 * indicates there are no running xids with an xid smaller than this.
	 */
	TransactionId initial_xmin_horizon;

	/* Indicates if we are building full snapshot or just catalog one. */
	bool		building_full_snapshot;

	/*
	 * Indicates if we are using the snapshot builder for the creation of a
	 * logical replication slot. If it's true, the start point for decoding
	 * changes is not determined yet. So we skip snapshot restores to properly
	 * find the start point. See SnapBuildFindSnapshot() for details.
	 */
	bool		in_slot_creation;

	/*
	 * Snapshot that's valid to see the catalog state seen at this moment.
	 */
	Snapshot	snapshot;

	/*
	 * LSN of the last location we are sure a snapshot has been serialized to.
	 */
	XLogRecPtr	last_serialized_snapshot;

	/*
	 * The reorderbuffer we need to update with usable snapshots et al.
	 */
	ReorderBuffer *reorder;

	/*
	 * TransactionId at which the next phase of initial snapshot building will
	 * happen. InvalidTransactionId if not known (i.e. SNAPBUILD_START), or
	 * when no next phase necessary (SNAPBUILD_CONSISTENT).
	 */
	TransactionId next_phase_at;

	/*
	 * Array of transactions which could have catalog changes that committed
	 * between xmin and xmax.
	 */
	struct
	{
		/* number of committed transactions */
		size_t		xcnt;

		/* available space for committed transactions */
		size_t		xcnt_space;

		/*
		 * Until we reach a CONSISTENT state, we record commits of all
		 * transactions, not just the catalog changing ones. Record when that
		 * changes so we know we cannot export a snapshot safely anymore.
		 */
		bool		includes_all_transactions;

		/*
		 * Array of committed transactions that have modified the catalog.
		 *
		 * As this array is frequently modified we do *not* keep it in
		 * xidComparator order. Instead we sort the array when building &
		 * distributing a snapshot.
		 *
		 * TODO: It's unclear whether that reasoning has much merit. Every
		 * time we add something here after becoming consistent will also
		 * require distributing a snapshot. Storing them sorted would
		 * potentially also make it easier to purge (but more complicated wrt
		 * wraparound?). Should be improved if sorting while building the
		 * snapshot shows up in profiles.
		 */
		TransactionId *xip;
	}			committed;

	/*
	 * Array of transactions and subtransactions that had modified catalogs
	 * and were running when the snapshot was serialized.
	 *
	 * We normally rely on some WAL record types such as HEAP2_NEW_CID to know
	 * if the transaction has changed the catalog. But it could happen that
	 * the logical decoding decodes only the commit record of the transaction
	 * after restoring the previously serialized snapshot in which case we
	 * will miss adding the xid to the snapshot and end up looking at the
	 * catalogs with the wrong snapshot.
	 *
	 * Now to avoid the above problem, we serialize the transactions that had
	 * modified the catalogs and are still running at the time of snapshot
	 * serialization. We fill this array while restoring the snapshot and then
	 * refer it while decoding commit to ensure if the xact has modified the
	 * catalog. We discard this array when all the xids in the list become old
	 * enough to matter. See SnapBuildPurgeOlderTxn for details.
	 */
	struct
	{
		/* number of transactions */
		size_t		xcnt;

		/* This array must be sorted in xidComparator order */
		TransactionId *xip;
	}			catchange;
};
```
## Detailed Description
SnapBuild manages the complex process of building consistent snapshots for logical replication in PostgreSQL. It tracks the progression through different states (START → BUILDING_SNAPSHOT → FULL_SNAPSHOT → CONSISTENT) while maintaining transaction visibility information and catalog change tracking.

The structure coordinates with the ReorderBuffer to provide consistent views of the database catalog during logical decoding, ensuring that decoded changes reflect a coherent state of the database. It handles both full snapshots for complete logical replication and catalog-only snapshots for more efficient operations.

Key responsibilities include:
- Tracking transaction visibility boundaries (xmin/xmax)
- Managing catalog change detection and recording
- Coordinating snapshot serialization and restoration
- Supporting two-phase commit protocols
- Maintaining consistency during slot creation

## Parameters / Member Variables
- : Current phase of snapshot building (SnapBuildState enum)
- : Private memory context for all allocations in this module
- : Lower bound - all transactions below this have committed/aborted
- : Upper bound - all transactions at or above this are uncommitted
- : LSN threshold below which commits should not be replayed
- : LSN where two-phase decoding was enabled or consistency found
- : Minimum xid threshold for starting WAL decoding
- : Flag indicating full vs catalog-only snapshot building
- : Flag indicating snapshot builder is for slot creation
- : Current valid snapshot for seeing catalog state
- : LSN of last confirmed snapshot serialization
- : Associated ReorderBuffer for snapshot coordination
- : Transaction ID triggering next snapshot building phase
- : Count of committed transactions with potential catalog changes
- : Allocated space in the committed transactions array
- : Whether all transactions are recorded (before CONSISTENT state)
- : Unsorted array of committed transaction IDs with catalog changes
- : Count of catalog-changing transactions running during serialization
- : Sorted array of transaction IDs that modified catalogs

## Dependencies
- Functions called/Symbols referenced:
  - [SnapBuildState](SnapBuildState.md) (enum for tracking build phases)
  - [ReorderBuffer](../R/ReorderBuffer.md) (coordination with transaction reordering)
  - [MemoryContext](../M/MemoryContext.md) (memory management)
  - [Snapshot](Snapshot.md) (PostgreSQL snapshot structure)
  - TransactionId, XLogRecPtr (core PostgreSQL types)

- Called from (representative examples):
  - [AllocateSnapshotBuilder](../A/AllocateSnapshotBuilder.md) (creates and initializes SnapBuild)
  - [SnapBuildProcessRunningXacts](SnapBuildProcessRunningXacts.md) (processes running transaction records)
  - [SnapBuildCommitTxn](SnapBuildCommitTxn.md) (handles transaction commit processing)
  - [SnapBuildSerialize](SnapBuildSerialize.md)/SnapBuildRestore (snapshot persistence)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (main logical decoding coordination)

## Notes and Other Information
- This structure is private to snapbuild.c and not exposed in public headers
- The committed.xip array is intentionally kept unsorted for performance during frequent modifications, only sorted when building snapshots
- The catchange array stores transactions that were running during serialization to handle cases where only commit records are decoded after snapshot restoration
- Two-phase commit support requires careful coordination with the two_phase_at LSN
- Memory management is handled through the dedicated context to ensure proper cleanup