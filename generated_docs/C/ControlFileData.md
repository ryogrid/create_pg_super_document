# ControlFileData

## Location
[src/include/catalog/pg_control.h:104-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_control.h#L104-L233)

## Overview
ControlFileData is the central data structure that defines the complete contents of PostgreSQL's pg_control file, serving as the authoritative source of cluster-wide configuration and state information essential for database startup, recovery, and compatibility verification.

## Definition

```c
typedef struct ControlFileData
{
	/*
	 * Unique system identifier --- to ensure we match up xlog files with the
	 * installation that produced them.
	 */
	uint64		system_identifier;

	/*
	 * Version identifier information.  Keep these fields at the same offset,
	 * especially pg_control_version; they won't be real useful if they move
	 * around.  (For historical reasons they must be 8 bytes into the file
	 * rather than immediately at the front.)
	 *
	 * pg_control_version identifies the format of pg_control itself.
	 * catalog_version_no identifies the format of the system catalogs.
	 *
	 * There are additional version identifiers in individual files; for
	 * example, WAL logs contain per-page magic numbers that can serve as
	 * version cues for the WAL log.
	 */
	uint32		pg_control_version; /* PG_CONTROL_VERSION */
	uint32		catalog_version_no; /* see catversion.h */

	/*
	 * System status data
	 */
	DBState		state;			/* see enum above */
	pg_time_t	time;			/* time stamp of last pg_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */

	CheckPoint	checkPointCopy; /* copy of last check point record */

	XLogRecPtr	unloggedLSN;	/* current fake LSN value, for unlogged rels */

	/*
	 * These two values determine the minimum point we must recover up to
	 * before starting up:
	 *
	 * minRecoveryPoint is updated to the latest replayed LSN whenever we
	 * flush a data change during archive recovery. That guards against
	 * starting archive recovery, aborting it, and restarting with an earlier
	 * stop location. If we've already flushed data changes from WAL record X
	 * to disk, we mustn't start up until we reach X again. Zero when not
	 * doing archive recovery.
	 *
	 * backupStartPoint is the redo pointer of the backup start checkpoint, if
	 * we are recovering from an online backup and haven't reached the end of
	 * backup yet. It is reset to zero when the end of backup is reached, and
	 * we mustn't start up before that. A boolean would suffice otherwise, but
	 * we use the redo pointer as a cross-check when we see an end-of-backup
	 * record, to make sure the end-of-backup record corresponds the base
	 * backup we're recovering from.
	 *
	 * backupEndPoint is the backup end location, if we are recovering from an
	 * online backup which was taken from the standby and haven't reached the
	 * end of backup yet. It is initialized to the minimum recovery point in
	 * pg_control which was backed up last. It is reset to zero when the end
	 * of backup is reached, and we mustn't start up before that.
	 *
	 * If backupEndRequired is true, we know for sure that we're restoring
	 * from a backup, and must see a backup-end record before we can safely
	 * start up.
	 */
	XLogRecPtr	minRecoveryPoint;
	TimeLineID	minRecoveryPointTLI;
	XLogRecPtr	backupStartPoint;
	XLogRecPtr	backupEndPoint;
	bool		backupEndRequired;

	/*
	 * Parameter settings that determine if the WAL can be used for archival
	 * or hot standby.
	 */
	int			wal_level;
	bool		wal_log_hints;
	int			MaxConnections;
	int			max_worker_processes;
	int			max_wal_senders;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	bool		track_commit_timestamp;

	/*
	 * This data is used to check for hardware-architecture compatibility of
	 * the database and the backend executable.  We need not check endianness
	 * explicitly, since the pg_control version will surely look wrong to a
	 * machine of different endianness, but we do need to worry about MAXALIGN
	 * and floating-point format.  (Note: storage layout nominally also
	 * depends on SHORTALIGN and INTALIGN, but in practice these are the same
	 * on all architectures of interest.)
	 *
	 * Testing just one double value is not a very bulletproof test for
	 * floating-point compatibility, but it will catch most cases.
	 */
	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */
#define FLOATFORMAT_VALUE	1234567.0

	/*
	 * This data is used to make sure that configuration of this database is
	 * compatible with the backend executable.
	 */
	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	uint32		toast_max_chunk_size;	/* chunk size in TOAST tables */
	uint32		loblksize;		/* chunk size in pg_largeobject */

	bool		float8ByVal;	/* float8, int8, etc pass-by-value? */

	/* Are data pages protected by checksums? Zero if no checksum version */
	uint32		data_checksum_version;

	/*
	 * Random nonce, used in authentication requests that need to proceed
	 * based on values that are cluster-unique, like a SASL exchange that
	 * failed at an early stage.
	 */
	char		mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];

	/* CRC of all above ... MUST BE LAST! */
	pg_crc32c	crc;
} ControlFileData;
```
## Detailed Description
ControlFileData represents the complete structure of PostgreSQL's pg_control file, which serves as the master control record for the entire database cluster. This structure contains critical information needed for database startup, crash recovery, compatibility verification, and operational parameters.

The pg_control file is the first file PostgreSQL reads during startup and contains authoritative information about the cluster's state, configuration, and recovery requirements. The structure is carefully designed with version controls and checksums to ensure data integrity and compatibility across different PostgreSQL versions and hardware architectures.

The structure includes several categories of information: system identification and versioning, current database state and checkpoint information, recovery control parameters, WAL configuration settings, hardware/architecture compatibility data, and database configuration parameters that affect storage layout and behavior.

## Parameters / Member Variables
- : Unique 64-bit identifier ensuring WAL files match the installation that produced them
- : Format version identifier for pg_control itself (must be PG_CONTROL_VERSION)
- : Format version identifier for system catalogs (from catversion.h)
- : Current database state (DB_STARTUP, DB_IN_PRODUCTION, DB_SHUTDOWNED, etc.)
- : Timestamp of the last pg_control file update
- : WAL record pointer to the last checkpoint record
- : Complete copy of the last checkpoint record for disaster recovery
- : Current fake LSN value used for unlogged relations
- : Minimum LSN that must be reached before database can start (archive recovery)
- : Timeline ID corresponding to minRecoveryPoint
- : Redo pointer of backup start checkpoint during online backup recovery
- : Backup end location for standby backups
- : Boolean indicating if backup-end record is required before startup
- : WAL logging level (minimal, replica, logical)
- : Whether WAL logging includes hint bit updates
- : Maximum number of concurrent connections
- : Maximum number of background worker processes
- : Maximum number of WAL sender processes
- : Maximum number of prepared transactions
- : Maximum number of locks per transaction
- : Whether commit timestamps are being tracked
- : Memory alignment requirement for tuples (architecture compatibility)
- : Floating-point format test value (1234567.0 for compatibility verification)
- : Data block size for this database (typically 8192 bytes)
- : Number of blocks per segment file for large relations
- : Block size within WAL files
- : Size of each WAL segment file
- : Width of catalog name fields (typically 64)
- : Maximum number of columns allowed in an index
- : Chunk size for TOAST (The Oversized-Attribute Storage Technique) tables
- : Chunk size for large objects stored in pg_largeobject
- : Whether 8-byte types (float8, int8) are passed by value
- : Data page checksum version (0 if checksums disabled)
- : Random nonce for authentication procedures
- : CRC32C checksum of all preceding fields (must be last field)

## Dependencies
- Functions called/Symbols referenced:
  - DBState (database state enumeration)
  - pg_time_t (timestamp type)
  - [CheckPoint](CheckPoint.md) (checkpoint record structure)
  - MOCK_AUTH_NONCE_LEN (authentication nonce length constant)
  - pg_crc32c (CRC checksum type)
  - XLogRecPtr (WAL record pointer type)
  - TimeLineID (timeline identifier type)

- Called from (representative examples):
  - [ReadControlFile](../R/ReadControlFile.md) (reads pg_control file)
  - [WriteControlFile](../W/WriteControlFile.md) (writes pg_control file)  
  - [InitControlFile](../I/InitControlFile.md) (initializes new pg_control file)
  - [get_controlfile](../g/get_controlfile.md) (common utility function)
  - [LocalProcessControlFile](../L/LocalProcessControlFile.md) (processes control file during startup)

## Notes and Other Information
- The pg_control file is critical for database startup and recovery; corruption can prevent database startup
- Version fields (pg_control_version, catalog_version_no) must match expected values or startup will fail
- The CRC field must be last and covers all preceding fields to ensure data integrity
- Hardware compatibility fields (maxAlign, floatFormat, float8ByVal) prevent startup on incompatible architectures
- The structure size affects PG_CONTROL_FILE_SIZE and must be carefully managed across PostgreSQL versions
- Changes to this structure typically require incrementing PG_CONTROL_VERSION
- The checkPointCopy field provides disaster recovery capability when WAL files are unavailable
- Recovery-related fields (minRecoveryPoint, backupStartPoint, etc.) control safe startup after backup restoration
- Configuration parameters stored here affect fundamental database behavior and storage layout
- The system_identifier ensures WAL files from different clusters cannot be accidentally mixed