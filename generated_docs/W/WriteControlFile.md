# WriteControlFile

## Location
[src/backend/access/transam/xlog.c:4216-4297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4216-L4297)

## Overview
Creates and writes the pg_control file to disk with system compatibility information, configuration parameters, and data integrity checksums during database initialization.

## Definition
static void WriteControlFile(void)

## Detailed Description
WriteControlFile is a static function that creates the pg_control file on disk during database cluster initialization. The function first populates the ControlFile buffer with version and compatibility-check fields including pg_control_version, catalog_version_no, data alignment parameters, block sizes, and various system constants. It then calculates and stores a CRC32C checksum to protect the file contents from corruption. The function writes exactly PG_CONTROL_FILE_SIZE bytes to the pg_control file, zero-padding any excess space beyond the actual ControlFileData structure size to reduce the likelihood of premature-EOF errors during reads. The write operation is performed with careful error handling and includes wait event reporting for monitoring. After writing, the function performs an fsync to ensure data durability before closing the file. Any failure during the process results in a PANIC, as the pg_control file is critical for database operation.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [BasicOpenFile](../B/BasicOpenFile.md)
  - write
  - [pg_fsync](../p/pg_fsync.md)
  - close
  - memset
  - memcpy
  - ereport
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - INIT_CRC32C
  - COMP_CRC32C
  - FIN_CRC32C
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [errmsg](../e/errmsg.md)
  - PG_CONTROL_VERSION
  - CATALOG_VERSION_NO
  - XLOG_CONTROL_FILE
  - PG_CONTROL_FILE_SIZE
- Called from (representative examples):
  - RefreshXLogWriteResult
  - [BootStrapXLOG](../B/BootStrapXLOG.md)

## Notes and Other Information
- The function creates the file with O_CREAT | O_EXCL flags, meaning it will fail if the file already exists
- Uses PG_BINARY flag for cross-platform compatibility
- Zero-padding to PG_CONTROL_FILE_SIZE helps prevent misleading error messages on partial reads
- CRC32C checksum provides data integrity verification for the critical control file
- Wait events WAIT_EVENT_CONTROL_FILE_WRITE and WAIT_EVENT_CONTROL_FILE_SYNC are reported for monitoring
- Any I/O error results in PANIC since pg_control is essential for database startup
- Must be called after InitControlFile() has prepared the ControlFile buffer
- The file is created in the data directory as "global/pg_control"

## Simplified Source

```c
static void
WriteControlFile(void)
{
    int fd;
    char buffer[PG_CONTROL_FILE_SIZE];

    // Set version and compatibility information
    ControlFile->pg_control_version = PG_CONTROL_VERSION;
    ControlFile->catalog_version_no = CATALOG_VERSION_NO;

    // Set system architecture parameters
    ControlFile->maxAlign = MAXIMUM_ALIGNOF;
    ControlFile->floatFormat = FLOATFORMAT_VALUE;
    ControlFile->blcksz = BLCKSZ;
    ControlFile->relseg_size = RELSEG_SIZE;
    ControlFile->xlog_blcksz = XLOG_BLCKSZ;
    ControlFile->xlog_seg_size = wal_segment_size;
    ControlFile->nameDataLen = NAMEDATALEN;
    ControlFile->indexMaxKeys = INDEX_MAX_KEYS;
    ControlFile->toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    ControlFile->loblksize = LOBLKSIZE;
    ControlFile->float8ByVal = FLOAT8PASSBYVAL;

    // Calculate and store CRC32C checksum for data integrity
    INIT_CRC32C(ControlFile->crc);
    COMP_CRC32C(ControlFile->crc, (char *) ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(ControlFile->crc);

    // Prepare write buffer with zero padding
    memset(buffer, 0, PG_CONTROL_FILE_SIZE);
    memcpy(buffer, ControlFile, sizeof(ControlFileData));

    // Create the pg_control file (must not already exist)
    fd = BasicOpenFile(XLOG_CONTROL_FILE, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
    if (fd < 0)
        ereport(PANIC, (errcode_for_file_access(),
                       errmsg("could not create file \"%s\": %m", XLOG_CONTROL_FILE)));

    // Write the control data with monitoring
    pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_WRITE);
    if (write(fd, buffer, PG_CONTROL_FILE_SIZE) != PG_CONTROL_FILE_SIZE) {
        if (errno == 0) errno = ENOSPC;  // Assume disk space issue
        ereport(PANIC, (errcode_for_file_access(),
                       errmsg("could not write to file \"%s\": %m", XLOG_CONTROL_FILE)));
    }
    pgstat_report_wait_end();

    // Ensure data is written to disk
    pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_SYNC);
    if (pg_fsync(fd) != 0)
        ereport(PANIC, (errcode_for_file_access(),
                       errmsg("could not fsync file \"%s\": %m", XLOG_CONTROL_FILE)));
    pgstat_report_wait_end();

    // Close the file
    if (close(fd) != 0)
        ereport(PANIC, (errcode_for_file_access(),
                       errmsg("could not close file \"%s\": %m", XLOG_CONTROL_FILE)));
}
```