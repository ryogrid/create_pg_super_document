# CreateDirAndVersionFile

## Location
[src/backend/commands/dbcommands.c:456-549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L456-L549)

## Overview
CreateDirAndVersionFile creates a database directory and writes the PG_VERSION file, handling both normal operations and WAL replay scenarios during database creation.

## Definition

```c
static void
CreateDirAndVersionFile(char *dbpath, Oid dbid, Oid tsid, bool isRedo)
```
## Detailed Description
This function performs the fundamental filesystem operations required to establish a new database directory structure. The function operates in two main phases:

**Directory and File Creation:**
1. Creates the database directory using MakePGDirectory, handling existing directories when in WAL replay mode
2. Creates a PG_VERSION file containing the PostgreSQL major version number
3. Uses OpenTransientFile with appropriate flags (O_WRONLY | O_CREAT | O_EXCL) for normal operation
4. In WAL replay mode, handles existing files by opening with O_TRUNC instead

**Data Writing and Synchronization:**
1. Writes the PG_MAJORVERSION string to the PG_VERSION file
2. Performs proper error handling for write operations, including disk space checks
3. Uses pgstat_report_wait_start/end for wait event reporting during I/O operations
4. Ensures data durability through pg_fsync on the file and fsync_fname on the directory
5. Properly closes the transient file handle

**WAL Logging (Non-Redo Mode):**
When not in WAL replay mode, the function generates a WAL record containing the database ID and tablespace ID using the XLog infrastructure for crash recovery.

## Parameters / Member Variables
- `*dbpath`: Filesystem path where the database directory should be created
- `dbid`: Database OID for the new database being created
- `tsid`: Tablespace OID where the database resides
- `isRedo`: Boolean indicating if this is being called during WAL replay
## Dependencies
- Functions called/Symbols referenced:
  - [MakePGDirectory](../M/MakePGDirectory.md): Creates PostgreSQL directory with proper permissions
  - [OpenTransientFile](../O/OpenTransientFile.md)/CloseTransientFile: File handle management for temporary operations
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end: Wait event reporting for monitoring
  - write: System call for writing version data to file
  - [pg_fsync](../p/pg_fsync.md): PostgreSQL wrapper for fsync system call
  - [fsync_fname](../f/fsync_fname.md): Synchronizes directory metadata changes
  - [data_sync_elevel](../d/data_sync_elevel.md): Determines appropriate error level for sync failures
  - [XLogBeginInsert](../X/XLogBeginInsert.md)/XLogRegisterData/XLogInsert: WAL record construction and insertion
- Called from (representative examples):
  - [CreateDatabaseUsingWalLog](CreateDatabaseUsingWalLog.md): Uses this during WAL_LOG database creation strategy
  - [dbase_redo](../d/dbase_redo.md): Uses this during WAL replay of database creation operations

## Notes and Other Information
- Static function within dbcommands.c, not exposed to external modules
- Handles both normal operation and WAL replay scenarios with appropriate error tolerance
- In WAL replay mode, tolerates existing directories and files, truncating version files as needed
- Ensures durability through proper fsync of both file contents and directory metadata
- Uses transient file handles to avoid long-term file descriptor consumption
- Generates WAL records only during normal operation, not during replay
- The PG_VERSION file always contains the current PostgreSQL major version, regardless of source
- Proper error handling with detailed error messages for filesystem operations
- Located at src/backend/commands/dbcommands.c:456-549

## Simplified Source

```c
static void
CreateDirAndVersionFile(char *dbpath, Oid dbid, Oid tsid, bool isRedo)
{
    int fd;
    int nbytes;
    char versionfile[MAXPGPATH];
    char buf[16];

    // Prepare version string
    sprintf(buf, "%s\n", PG_MAJORVERSION);
    nbytes = strlen(PG_MAJORVERSION) + 1;

    // Create database directory
    if (MakePGDirectory(dbpath) < 0) {
        if (errno != EEXIST || !isRedo)
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not create directory \"%s\": %m", dbpath)));
    }

    // Create PG_VERSION file
    snprintf(versionfile, sizeof(versionfile), "%s/%s", dbpath, "PG_VERSION");

    fd = OpenTransientFile(versionfile, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY);
    if (fd < 0 && errno == EEXIST && isRedo)
        fd = OpenTransientFile(versionfile, O_WRONLY | O_TRUNC | PG_BINARY);

    if (fd < 0)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not create file \"%s\": %m", versionfile)));

    // Write version data with proper error handling
    pgstat_report_wait_start(WAIT_EVENT_VERSION_FILE_WRITE);
    errno = 0;
    if ((int) write(fd, buf, nbytes) != nbytes) {
        if (errno == 0)
            errno = ENOSPC;
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not write to file \"%s\": %m", versionfile)));
    }
    pgstat_report_wait_end();

    // Sync file and directory
    pgstat_report_wait_start(WAIT_EVENT_VERSION_FILE_SYNC);
    if (pg_fsync(fd) != 0)
        ereport(data_sync_elevel(ERROR), (errcode_for_file_access(),
                errmsg("could not fsync file \"%s\": %m", versionfile)));
    fsync_fname(dbpath, true);
    pgstat_report_wait_end();

    CloseTransientFile(fd);

    // Write WAL record if not in replay mode
    if (!isRedo) {
        xl_dbase_create_wal_log_rec xlrec;

        START_CRIT_SECTION();

        xlrec.db_id = dbid;
        xlrec.tablespace_id = tsid;

        XLogBeginInsert();
        XLogRegisterData((char *) (&xlrec), sizeof(xl_dbase_create_wal_log_rec));
        (void) XLogInsert(RM_DBASE_ID, XLOG_DBASE_CREATE_WAL_LOG);

        END_CRIT_SECTION();
    }
}
```