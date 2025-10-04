# BootStrapXLOG

## Location
[src/backend/access/transam/xlog.c:4988-5153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4988-L5153)

## Overview
Initializes the PostgreSQL Write-Ahead Logging (WAL) system during database installation by creating the initial pg_control file and the first XLOG segment with a bootstrap checkpoint record.

## Definition

```c
struct timeval tv;
```
## Detailed Description
BootStrapXLOG is a critical function that must be called exactly once during PostgreSQL system installation. It performs the fundamental initialization of the WAL system by:

1. **System Identifier Generation**: Creates a unique system identifier using gettimeofday() combined with the process ID to ensure installation uniqueness
2. **Initial Checkpoint Setup**: Configures the bootstrap checkpoint record with default values for transaction IDs, object IDs, and timeline information
3. **WAL Page Creation**: Constructs the first WAL page with proper headers and the initial checkpoint record
4. **File System Operations**: Creates the first XLOG segment file (000000010000000000000001) and writes the initial page
5. **Control File Initialization**: Creates and writes the pg_control file with system metadata
6. **Subsystem Bootstrap**: Initializes related transaction management subsystems (CLOG, CommitTs, SUBTRANS, MultiXact)

The function establishes the foundational WAL infrastructure that all subsequent database operations depend upon. It creates WAL segment 0/1 (the first segment 0/0 is intentionally unused to allow 0/0 to represent "before any valid WAL segment").

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [SetInstallXLogFileSegmentActive](../S/SetInstallXLogFileSegmentActive.md): Enables WAL segment creation
  - [gettimeofday](../g/gettimeofday.md): Used for system identifier generation
  - [XLogFileInit](../X/XLogFileInit.md): Creates the initial XLOG segment file
  - [InitControlFile](../I/InitControlFile.md): Initializes the control file structure
  - [WriteControlFile](../W/WriteControlFile.md): Writes control file to disk
  - [BootStrapCLOG](BootStrapCLOG.md): Initializes commit log
  - [BootStrapCommitTs](BootStrapCommitTs.md): Initializes commit timestamp subsystem
  - [BootStrapSUBTRANS](BootStrapSUBTRANS.md): Initializes subtransaction subsystem
  - [BootStrapMultiXact](BootStrapMultiXact.md): Initializes multitransaction subsystem
  - [ReadControlFile](../R/ReadControlFile.md): Forces control file validation

- Called from (representative examples):
  - [BootstrapModeMain](BootstrapModeMain.md): Main bootstrap process entry point

## Notes and Other Information
- This function must be called only once during the lifetime of a PostgreSQL installation
- Creates the first WAL segment with timeline ID 1 (BootstrapTimeLineID)
- The initial checkpoint record is of type XLOG_CHECKPOINT_SHUTDOWN
- Uses O_DIRECT-aligned buffers for optimal I/O performance
- Includes comprehensive error handling with PANIC-level messages for critical failures
- The system identifier encoding allows determination of installation time from the database
- Forces synchronous writes and fsync operations to ensure data durability during bootstrap

## Simplified Source

```c
void
BootStrapXLOG(void)
{
    CheckPoint checkpoint;
    char *buffer;
    XLogPageHeader page;
    XLogRecord *record;
    uint64 sysidentifier;
    struct timeval tv;

    // Enable WAL segment creation
    SetInstallXLogFileSegmentActive();

    // Generate unique system identifier using timestamp + PID
    gettimeofday(&tv, NULL);
    sysidentifier = ((uint64) tv.tv_sec) << 32;
    sysidentifier |= ((uint64) tv.tv_usec) << 12;
    sysidentifier |= getpid() & 0xFFF;

    // Allocate and initialize WAL page buffer
    buffer = (char *) palloc(XLOG_BLCKSZ + XLOG_BLCKSZ);
    page = (XLogPageHeader) TYPEALIGN(XLOG_BLCKSZ, buffer);
    memset(page, 0, XLOG_BLCKSZ);

    // Setup initial checkpoint record with bootstrap values
    checkpoint.redo = wal_segment_size + SizeOfXLogLongPHD;
    checkpoint.ThisTimeLineID = BootstrapTimeLineID;
    checkpoint.nextXid = FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId);
    checkpoint.nextOid = FirstGenbkiObjectId;
    checkpoint.time = (pg_time_t) time(NULL);
    // ... other checkpoint fields initialized

    // Update global transaction state
    TransamVariables->nextXid = checkpoint.nextXid;
    TransamVariables->nextOid = checkpoint.nextOid;

    // Setup WAL page header with system info
    page->xlp_magic = XLOG_PAGE_MAGIC;
    page->xlp_info = XLP_LONG_HEADER;
    page->xlp_tli = BootstrapTimeLineID;
    ((XLogLongPageHeader) page)->xlp_sysid = sysidentifier;

    // Insert checkpoint record into page
    record = (XLogRecord *) ((char *) page + SizeOfXLogLongPHD);
    record->xl_info = XLOG_CHECKPOINT_SHUTDOWN;
    record->xl_rmid = RM_XLOG_ID;
    // ... copy checkpoint data and calculate CRC

    // Create and write first XLOG segment file
    openLogFile = XLogFileInit(1, BootstrapTimeLineID);
    write(openLogFile, page, XLOG_BLCKSZ);
    pg_fsync(openLogFile);
    close(openLogFile);

    // Initialize control file with system metadata
    InitControlFile(sysidentifier);
    ControlFile->checkPoint = checkpoint.redo;
    ControlFile->checkPointCopy = checkpoint;
    WriteControlFile();

    // Bootstrap related transaction subsystems
    BootStrapCLOG();
    BootStrapCommitTs();
    BootStrapSUBTRANS();
    BootStrapMultiXact();

    pfree(buffer);
    ReadControlFile();  // Force validation
}
```