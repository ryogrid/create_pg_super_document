# ControlFileData

## Location
src/include/catalog/pg_control.h: 104 - 233

## Overview
ControlFileData is the central data structure that defines the complete contents of PostgreSQL's pg_control file, serving as the authoritative source of cluster-wide configuration and state information essential for database startup, recovery, and compatibility verification.

## Definition


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
  - CheckPoint (checkpoint record structure)
  - MOCK_AUTH_NONCE_LEN (authentication nonce length constant)
  - pg_crc32c (CRC checksum type)
  - XLogRecPtr (WAL record pointer type)
  - TimeLineID (timeline identifier type)

- Called from (representative examples):
  - ReadControlFile (reads pg_control file)
  - WriteControlFile (writes pg_control file)  
  - InitControlFile (initializes new pg_control file)
  - get_controlfile (common utility function)
  - LocalProcessControlFile (processes control file during startup)

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