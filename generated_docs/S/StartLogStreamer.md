# StartLogStreamer

## Location
[src/bin/pg_basebackup/pg_basebackup.c:616-746](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L616-L746)

## Overview
Initiates a background process or thread for receiving WAL (Write-Ahead Log) data during a base backup, enabling parallel streaming of transaction logs while the backup is being performed.

## Definition
```c
static void StartLogStreamer(char *startpos, uint32 timeline, char *sysidentifier,
                           pg_compress_algorithm wal_compress_algorithm, 
                           int wal_compress_level)
```

## Detailed Description
`StartLogStreamer` sets up and launches the background WAL streaming mechanism for pg_basebackup operations. It parses the starting WAL position, establishes a separate database connection for streaming, creates necessary directories for WAL storage, and handles replication slot creation when required. The function implements platform-specific process creation: using fork() on Unix systems and _beginthreadex() on Windows. It properly handles version-specific differences in PostgreSQL server capabilities, including the pg_xlog to pg_wal directory rename and support for temporary replication slots and WAL summaries.

## Parameters / Member Variables
- `startpos`: String representation of the WAL position to start streaming from (format: "X/X")  
- `timeline`: Timeline ID for the WAL stream
- `sysidentifier`: System identifier string for validation and connection setup
- `wal_compress_algorithm`: Algorithm to use for compressing WAL data during streaming
- `wal_compress_level`: Compression level setting for the specified algorithm

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md) (allocates zero-initialized memory)
  - sscanf (parses WAL position string)
  - XLogSegmentOffset (calculates WAL segment offset)
  - pipe (creates background communication pipe on Unix)
  - [GetConnection](../G/GetConnection.md) (establishes database connection)
  - [PQserverVersion](../P/PQserverVersion.md) (gets PostgreSQL server version)
  - [PQbackendPID](../P/PQbackendPID.md) (gets backend process ID)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (creates temporary or permanent replication slot)
  - [pg_mkdir_p](../p/pg_mkdir_p.md) (creates directory hierarchy)
  - [LogStreamerMain](../L/LogStreamerMain.md) (main function for the background process/thread)
  - fork/_beginthreadex (platform-specific process/thread creation)
  - [kill_bgchild_atexit](../k/kill_bgchild_atexit.md) (cleanup function for Unix)
  - pg_log_info (logging function)
- Global variables accessed:
  - bgpipe, bgchild (process communication and control)
  - basedir, conn (backup configuration)
  - temp_replication_slot, replication_slot, create_slot (replication settings)
  - format, verbose (backup options)
- Called from (representative examples):
  - [BaseBackup](../B/BaseBackup.md) function in pg_basebackup.c at line 2119

## Notes and Other Information
- Creates a separate database connection for WAL streaming to enable parallel operation with the base backup
- Handles PostgreSQL version compatibility, including pg_xlog to pg_wal directory rename in version 10+
- Automatically creates temporary replication slots with unique names based on backend PID when requested
- Sets up proper directory structure including archive_status and summaries subdirectories for WAL management
- Rounds the starting position down to segment boundary for proper WAL streaming alignment
- Uses platform-specific mechanisms: Unix fork() creates separate process, Windows _beginthreadex() creates thread
- Registers cleanup handler (kill_bgchild_atexit) on Unix to ensure background process termination
- Critical for pg_basebackup's ability to maintain WAL continuity during backup operations

## Simplified Source

```c
static void
StartLogStreamer(char *startpos, uint32 timeline, char *sysidentifier,
                 pg_compress_algorithm wal_compress_algorithm, int wal_compress_level)
{
    logstreamer_param *param;
    uint32 hi, lo;
    char statusdir[MAXPGPATH];

    // Allocate and initialize parameter structure
    param = pg_malloc0(sizeof(logstreamer_param));
    param->timeline = timeline;
    param->sysidentifier = sysidentifier;
    param->wal_compress_algorithm = wal_compress_algorithm;
    param->wal_compress_level = wal_compress_level;

    // Parse starting WAL position and round to segment boundary
    if (sscanf(startpos, "%X/%X", &hi, &lo) != 2)
        pg_fatal("could not parse write-ahead log location \"%s\"", startpos);
    param->startptr = ((uint64) hi) << 32 | lo;
    param->startptr -= XLogSegmentOffset(param->startptr, WalSegSz);

#ifndef WIN32
    // Create background pipe for communication
    if (pipe(bgpipe) < 0)
        pg_fatal("could not create pipe for background process: %m");
#endif

    // Get database connection for WAL streaming
    param->bgconn = GetConnection();
    if (!param->bgconn)
        exit(1);

    // Set up WAL directory path (pg_xlog vs pg_wal based on server version)
    snprintf(param->xlog, sizeof(param->xlog), "%s/%s", basedir,
             PQserverVersion(conn) < MINIMUM_VERSION_FOR_PG_WAL ? "pg_xlog" : "pg_wal");

    // Create replication slot if needed
    if (temp_replication_slot && !replication_slot)
        replication_slot = psprintf("pg_basebackup_%u", (unsigned int) PQbackendPID(param->bgconn));

    if (temp_replication_slot || create_slot) {
        if (!CreateReplicationSlot(param->bgconn, replication_slot, NULL,
                                   temp_replication_slot, true, true, false, false))
            exit(1);
        if (verbose)
            pg_log_info("created %sreplication slot \"%s\"",
                        temp_replication_slot ? "temporary " : "", replication_slot);
    }

    // Create necessary directories for plain format
    if (format == 'p') {
        snprintf(statusdir, sizeof(statusdir), "%s/%s/archive_status", basedir,
                 PQserverVersion(conn) < MINIMUM_VERSION_FOR_PG_WAL ? "pg_xlog" : "pg_wal");
        if (pg_mkdir_p(statusdir, pg_dir_create_mode) != 0 && errno != EEXIST)
            pg_fatal("could not create directory \"%s\": %m", statusdir);

        // Create summaries directory for newer servers
        if (PQserverVersion(conn) >= MINIMUM_VERSION_FOR_WAL_SUMMARIES) {
            char summarydir[MAXPGPATH];
            snprintf(summarydir, sizeof(summarydir), "%s/%s/summaries", basedir, "pg_wal");
            if (pg_mkdir_p(summarydir, pg_dir_create_mode) != 0 && errno != EEXIST)
                pg_fatal("could not create directory \"%s\": %m", summarydir);
        }
    }

    // Start background process/thread
#ifndef WIN32
    bgchild = fork();
    if (bgchild == 0) {
        // Child process - run log streamer
        exit(LogStreamerMain(param));
    }
    else if (bgchild < 0) {
        pg_fatal("could not create background process: %m");
    }
    atexit(kill_bgchild_atexit);
#else
    bgchild = _beginthreadex(NULL, 0, (void *) LogStreamerMain, param, 0, NULL);
    if (bgchild == 0)
        pg_fatal("could not create background thread: %m");
#endif
}
```