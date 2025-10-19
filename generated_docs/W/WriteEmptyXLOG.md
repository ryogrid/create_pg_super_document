# WriteEmptyXLOG

## Location
[src/bin/pg_resetwal/pg_resetwal.c:1079-1165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_resetwal/pg_resetwal.c#L1079-L1165)

## Overview
WriteEmptyXLOG creates a new, properly formatted WAL segment file containing only an initial checkpoint record to bootstrap the transaction log after a reset operation.

## Definition
```c
static void WriteEmptyXLOG(void)
```

## Detailed Description
This static function is the final step in the pg_resetwal process that creates a brand new WAL segment file to replace the old transaction log. The function constructs a properly formatted WAL file that contains only the essential initial checkpoint record needed to bootstrap PostgreSQL's transaction log system.

The function performs several critical operations:
1. Sets up a properly formatted WAL page header with magic numbers and system identifiers
2. Creates an initial checkpoint record containing the checkpoint data from the control file
3. Calculates and sets the CRC checksum for data integrity
4. Writes the first page containing the checkpoint record
5. Fills the remainder of the WAL segment with zeros
6. Ensures data is written to disk with fsync

The resulting file serves as the foundation for all future transaction logging in the reset database.

## Parameters / Member Variables
This function takes no parameters and operates on:
- Global ControlFile structure (reads checkpoint data and system information)
- Global newXlogSegNo (determines the filename for the new WAL segment)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogFilePath](../X/XLogFilePath.md) (generates the WAL file path)
  - unlink (removes any existing file at the path)
  - open (creates the new WAL file)
  - write (writes data to the file)
  - fsync (ensures data is written to disk)
  - close (closes the file)
  - memset, memcpy (memory operations)
  - INIT_CRC32C, COMP_CRC32C, FIN_CRC32C (CRC calculation macros)
  - PGAlignedXLogBlock (aligned buffer for WAL data)
  - XLogPageHeader, XLogLongPageHeader (WAL page header structures)
  - [XLogRecord](../X/XLogRecord.md) (WAL record structure)
  - Various constants: XLOG_PAGE_MAGIC, XLP_LONG_HEADER, XLOG_CHECKPOINT_SHUTDOWN, etc.

- Called from:
  - [main](../m/main.md) (in pg_resetwal.c at line 498)

## Notes and Other Information
- This is a static function local to pg_resetwal.c
- Creates a WAL segment file of size WalSegSz (typically 16MB)
- The first page contains the checkpoint record, remaining pages are zeroed
- Uses proper WAL format with page headers and record structures
- Includes comprehensive error handling for file operations
- The file is created with O_EXCL flag to ensure it doesn't overwrite existing files
- Critical for database bootstrap - without this file, PostgreSQL cannot start
- The checkpoint record contains all necessary information for database recovery
- Part of the final "construction" phase after all destructive operations are complete
- The fsync call ensures durability and that the database can reliably start after reset
- The function handles potential disk space issues with appropriate error messages

## Simplified Source

```c
static void WriteEmptyXLOG(void)
{
    PGAlignedXLogBlock buffer;
    XLogPageHeader page;
    XLogLongPageHeader longpage;
    XLogRecord *record;
    pg_crc32c crc;
    char path[MAXPGPATH];
    int fd;
    int nbytes;
    char *recptr;

    // Initialize buffer
    memset(buffer.data, 0, XLOG_BLCKSZ);

    // Set up WAL page header with system info
    page = (XLogPageHeader) buffer.data;
    page->xlp_magic = XLOG_PAGE_MAGIC;
    page->xlp_info = XLP_LONG_HEADER;
    page->xlp_tli = ControlFile.checkPointCopy.ThisTimeLineID;
    page->xlp_pageaddr = ControlFile.checkPointCopy.redo - SizeOfXLogLongPHD;

    longpage = (XLogLongPageHeader) page;
    longpage->xlp_sysid = ControlFile.system_identifier;
    longpage->xlp_seg_size = WalSegSz;
    longpage->xlp_xlog_blcksz = XLOG_BLCKSZ;

    // Create initial checkpoint record
    recptr = (char *) page + SizeOfXLogLongPHD;
    record = (XLogRecord *) recptr;
    record->xl_prev = 0;
    record->xl_xid = InvalidTransactionId;
    record->xl_tot_len = SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + sizeof(CheckPoint);
    record->xl_info = XLOG_CHECKPOINT_SHUTDOWN;
    record->xl_rmid = RM_XLOG_ID;

    // Add checkpoint data
    recptr += SizeOfXLogRecord;
    *(recptr++) = (char) XLR_BLOCK_ID_DATA_SHORT;
    *(recptr++) = sizeof(CheckPoint);
    memcpy(recptr, &ControlFile.checkPointCopy, sizeof(CheckPoint));

    // Calculate and set CRC
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord,
                record->xl_tot_len - SizeOfXLogRecord);
    COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(crc);
    record->xl_crc = crc;

    // Create and write new WAL file
    XLogFilePath(path, ControlFile.checkPointCopy.ThisTimeLineID,
                 newXlogSegNo, WalSegSz);
    unlink(path);

    fd = open(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
    if (fd < 0)
        pg_fatal("could not open file \"%s\": %m", path);

    // Write first page with checkpoint
    if (write(fd, buffer.data, XLOG_BLCKSZ) != XLOG_BLCKSZ)
        pg_fatal("could not write file \"%s\": %m", path);

    // Fill rest of segment with zeros
    memset(buffer.data, 0, XLOG_BLCKSZ);
    for (nbytes = XLOG_BLCKSZ; nbytes < WalSegSz; nbytes += XLOG_BLCKSZ)
    {
        if (write(fd, buffer.data, XLOG_BLCKSZ) != XLOG_BLCKSZ)
            pg_fatal("could not write file \"%s\": %m", path);
    }

    // Ensure data is written to disk
    if (fsync(fd) != 0)
        pg_fatal("fsync error: %m");
    close(fd);
}
```