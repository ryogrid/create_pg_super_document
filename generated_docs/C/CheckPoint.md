# CheckPoint

## Location
[src/include/catalog/pg_control.h:35-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_control.h#L35-L65)

## Overview
CheckPoint is a critical data structure that represents the body of checkpoint XLOG records in PostgreSQL, containing essential recovery and consistency information that is stored both in WAL records and in pg_control for disaster recovery purposes.

## Definition

```c
typedef struct CheckPoint
{
	XLogRecPtr	redo;			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	TimeLineID	ThisTimeLineID; /* current TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI, if this record begins a new
								 * timeline (equals ThisTimeLineID otherwise) */
	bool		fullPageWrites; /* current full_page_writes */
	int			wal_level;		/* current wal_level */
	FullTransactionId nextXid;	/* next free transaction ID */
	Oid			nextOid;		/* next free OID */
	MultiXactId nextMulti;		/* next free MultiXactId */
	MultiXactOffset nextMultiOffset;	/* next free MultiXact offset */
	TransactionId oldestXid;	/* cluster-wide minimum datfrozenxid */
	Oid			oldestXidDB;	/* database with minimum datfrozenxid */
	MultiXactId oldestMulti;	/* cluster-wide minimum datminmxid */
	Oid			oldestMultiDB;	/* database with minimum datminmxid */
	pg_time_t	time;			/* time stamp of checkpoint */
	TransactionId oldestCommitTsXid;	/* oldest Xid with valid commit
										 * timestamp */
	TransactionId newestCommitTsXid;	/* newest Xid with valid commit
										 * timestamp */

	/*
	 * Oldest XID still running. This is only needed to initialize hot standby
	 * mode from an online checkpoint, so we only bother calculating this for
	 * online checkpoints and only when wal_level is replica. Otherwise it's
	 * set to InvalidTransactionId.
	 */
	TransactionId oldestActiveXid;
} CheckPoint;
```
## Detailed Description
The CheckPoint structure serves as the fundamental data container for PostgreSQL's checkpoint mechanism, which is crucial for database recovery and consistency. This structure encapsulates the complete state of the database at a specific point in time, including transaction boundaries, resource allocation counters, and recovery information.

The structure is designed to be written to WAL (Write-Ahead Log) records during checkpoint operations and is also maintained in the pg_control file for disaster recovery scenarios. Any modifications to this structure require incrementing the PG_CONTROL_VERSION to ensure compatibility across PostgreSQL versions.

The checkpoint data enables PostgreSQL to determine the starting point for crash recovery (the redo point) and maintains critical system-wide state information including transaction ID allocation, timeline management, and freeze boundaries for vacuum operations.

## Parameters / Member Variables
- `redo`: The WAL record pointer indicating where crash recovery should begin; represents the next available RecPtr when checkpoint creation started
- `ThisTimeLineID`: Current timeline identifier, used for point-in-time recovery and replication scenarios
- `PrevTimeLineID`: Previous timeline identifier; equals ThisTimeLineID unless this checkpoint begins a new timeline
- `fullPageWrites`: Current setting of the full_page_writes parameter, affecting WAL logging behavior
- `wal_level`: Current WAL logging level (minimal, replica, or logical), determining what information is logged
- `nextXid`: Next available full transaction ID to be assigned to new transactions
- `nextOid`: Next available object identifier for new database objects
- `nextMulti`: Next available MultiXactId for tuple locking operations involving multiple transactions
- `nextMultiOffset`: Next available offset in the MultiXact member space
- `oldestXid`: Cluster-wide minimum datfrozenxid value, used for vacuum freeze horizon calculations
- `oldestXidDB`: Database OID containing the minimum datfrozenxid value
- `oldestMulti`: Cluster-wide minimum datminmxid value for MultiXact cleanup
- `oldestMultiDB`: Database OID containing the minimum datminmxid value
- `time`: Timestamp when the checkpoint was created
- `oldestCommitTsXid`: Oldest transaction ID with a valid commit timestamp (for commit timestamp tracking)
- `newestCommitTsXid`: Newest transaction ID with a valid commit timestamp
- `oldestActiveXid`: Oldest transaction ID that was still running at checkpoint time (used for hot standby initialization)
## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionId](../F/FullTransactionId.md)
  - MultiXactId
  - MultiXactOffset
  - pg_time_t
  - XLogRecPtr
  - TimeLineID
  - TransactionId
  - Oid

- Called from (representative examples):
  - [CreateCheckPoint](CreateCheckPoint.md) (main checkpoint creation function)
  - [StartupXLOG](../S/StartupXLOG.md) (during crash recovery)
  - [xlog_redo](../x/xlog_redo.md) (during WAL replay)
  - [ReadCheckpointRecord](../R/ReadCheckpointRecord.md) (checkpoint record processing)
  - [ControlFileData](ControlFileData.md) (stored in pg_control file)

## Notes and Other Information
- This structure is stored in both WAL records and the pg_control file, making it critical for disaster recovery
- The oldestActiveXid field is only calculated for online checkpoints when wal_level is replica or higher
- Changes to this structure require a PG_CONTROL_VERSION increment due to its storage in pg_control
- The structure serves as a consistency point that enables PostgreSQL to determine safe starting points for recovery operations
- Timeline information supports point-in-time recovery and streaming replication scenarios
- Transaction ID and MultiXact tracking prevents wraparound issues and ensures proper cleanup operations