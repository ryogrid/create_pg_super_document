# ReadControlFile

## Location
src/backend/access/transam/xlog.c: 4298 - 4513

## Overview
Reads and validates the pg_control file during database startup, performing extensive compatibility checks to ensure the database files match the server executable's compilation parameters.

## Definition
static void ReadControlFile(void)

## Detailed Description
ReadControlFile is a static function that loads the pg_control file from disk during postmaster or standalone backend startup and performs comprehensive validation of its contents. The function first opens and reads the control file, then performs a series of critical checks: CRC validation to detect corruption, pg_control_version verification, catalog_version_no compatibility, and numerous compile-time parameter comparisons including data alignment, block sizes, floating-point format, and other architectural constants. If any incompatibility is detected, the function reports a FATAL error suggesting the user needs to run initdb or recompile. After successful validation, it sets the global wal_segment_size variable, validates WAL segment size constraints, updates configuration options, calculates checkpoint segments, and makes initdb settings visible as GUC variables. This function is essential for ensuring that database files created with one PostgreSQL configuration can only be used with compatible server executables.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - BasicOpenFile
  - read
  - close
  - ereport
  - pgstat_report_wait_start
  - pgstat_report_wait_end
  - INIT_CRC32C
  - COMP_CRC32C
  - FIN_CRC32C
  - EQ_CRC32C
  - IsValidWalSegSize
  - ConvertToXSegs
  - CalculateCheckpointSegments
  - SetConfigOption
  - DataChecksumsEnabled
  - snprintf
  - errcode_for_file_access
  - errmsg
  - errdetail
  - errhint
  - errmsg_plural
  - XLOG_CONTROL_FILE
  - PG_CONTROL_VERSION
  - CATALOG_VERSION_NO
- Called from (representative examples):
  - RefreshXLogWriteResult
  - LocalProcessControlFile
  - BootStrapXLOG

## Notes and Other Information
- Performs wait event reporting with WAIT_EVENT_CONTROL_FILE_READ for monitoring
- Validates over a dozen compile-time compatibility parameters including BLCKSZ, RELSEG_SIZE, XLOG_BLCKSZ, NAMEDATALEN, INDEX_MAX_KEYS, etc.
- Special handling for byte-order detection in pg_control_version checking
- Sets global wal_segment_size based on control file contents and validates min/max WAL size constraints
- Updates UsableBytesInSegment calculation based on WAL segment size
- Makes data_checksums setting visible as a GUC variable
- Any compatibility mismatch results in FATAL error with detailed suggestions for resolution
- CRC validation protects against corruption of the critical control file
- Essential for preventing database corruption from mismatched server/data configurations