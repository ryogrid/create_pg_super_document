# ReadControlFile

## Location
[src/backend/access/transam/xlog.c:4298-4513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4298-L4513)

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
  - [BasicOpenFile](../B/BasicOpenFile.md)
  - read
  - close
  - ereport
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - INIT_CRC32C
  - COMP_CRC32C
  - FIN_CRC32C
  - EQ_CRC32C
  - IsValidWalSegSize
  - ConvertToXSegs
  - [CalculateCheckpointSegments](../C/CalculateCheckpointSegments.md)
  - [SetConfigOption](../S/SetConfigOption.md)
  - [DataChecksumsEnabled](../D/DataChecksumsEnabled.md)
  - snprintf
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
  - [errhint](../e/errhint.md)
  - [errmsg_plural](../e/errmsg_plural.md)
  - XLOG_CONTROL_FILE
  - PG_CONTROL_VERSION
  - CATALOG_VERSION_NO
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [LocalProcessControlFile](../L/LocalProcessControlFile.md)
  - [BootStrapXLOG](../B/BootStrapXLOG.md)

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

## Simplified Source

```c
// Simplified version of ReadControlFile
static void ReadControlFile(void) {
    pg_crc32c crc;
    int fd;
    int read_result;

    // Step 1: Open and read the control file
    fd = BasicOpenFile(XLOG_CONTROL_FILE, O_RDWR | PG_BINARY);
    if (fd < 0) {
        ereport(PANIC, "could not open control file");
    }

    pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_READ);
    read_result = read(fd, ControlFile, sizeof(ControlFileData));
    if (read_result != sizeof(ControlFileData)) {
        ereport(PANIC, "could not read control file completely");
    }
    pgstat_report_wait_end();
    close(fd);

    // Step 2: Validate pg_control version compatibility
    if (ControlFile->pg_control_version != PG_CONTROL_VERSION) {
        ereport(FATAL, "database files are incompatible with server - version mismatch");
    }

    // Step 3: Verify CRC checksum to detect corruption
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char *) ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, ControlFile->crc)) {
        ereport(FATAL, "incorrect checksum in control file");
    }

    // Step 4: Check critical compatibility parameters
    if (ControlFile->catalog_version_no != CATALOG_VERSION_NO ||
        ControlFile->maxAlign != MAXIMUM_ALIGNOF ||
        ControlFile->floatFormat != FLOATFORMAT_VALUE ||
        ControlFile->blcksz != BLCKSZ ||
        ControlFile->relseg_size != RELSEG_SIZE ||
        ControlFile->xlog_blcksz != XLOG_BLCKSZ ||
        ControlFile->nameDataLen != NAMEDATALEN ||
        ControlFile->indexMaxKeys != INDEX_MAX_KEYS ||
        ControlFile->toast_max_chunk_size != TOAST_MAX_CHUNK_SIZE ||
        ControlFile->loblksize != LOBLKSIZE) {
        ereport(FATAL, "database files are incompatible with server - parameter mismatch");
    }

    // Step 5: Check floating-point representation compatibility
#ifdef USE_FLOAT8_BYVAL
    if (ControlFile->float8ByVal != true) {
        ereport(FATAL, "float8 byval compatibility mismatch");
    }
#else
    if (ControlFile->float8ByVal != false) {
        ereport(FATAL, "float8 byval compatibility mismatch");
    }
#endif

    // Step 6: Set up WAL segment size and validate constraints
    wal_segment_size = ControlFile->xlog_seg_size;

    if (!IsValidWalSegSize(wal_segment_size)) {
        ereport(ERROR, "invalid WAL segment size in control file");
    }

    // Step 7: Update configuration and calculate derived values
    SetConfigOption("wal_segment_size", wal_segment_size_string, PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);

    // Validate min/max WAL size constraints
    if (ConvertToXSegs(min_wal_size_mb, wal_segment_size) < 2 ||
        ConvertToXSegs(max_wal_size_mb, wal_segment_size) < 2) {
        ereport(ERROR, "WAL size parameters incompatible with segment size");
    }

    // Calculate usable bytes per segment
    UsableBytesInSegment = (wal_segment_size / XLOG_BLCKSZ * UsableBytesInPage) -
                          (SizeOfXLogLongPHD - SizeOfXLogShortPHD);

    // Finalize setup
    CalculateCheckpointSegments();
    SetConfigOption("data_checksums", DataChecksumsEnabled() ? "yes" : "no",
                   PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);
}
```

Key simplifications made:
- Consolidated multiple similar compatibility checks into a single compound condition
- Removed detailed error message formatting for clarity
- Abstracted individual parameter checks into a unified validation step
- Simplified floating-point compatibility check logic
- Combined WAL size validation checks
- Focused on the main execution path while preserving all critical validation steps
- Maintained the essential algorithm: read file → validate version → check CRC → verify compatibility → configure WAL settings