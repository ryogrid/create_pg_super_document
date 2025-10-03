# heap_xlog_logical_rewrite

## Location
[src/backend/access/heap/rewriteheap.c:1073-1154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L1073-L1154)

## Overview
Replays XLOG_HEAP2_REWRITE records during WAL recovery by reconstructing logical rewrite mapping files used for logical decoding after table rewrites.

## Definition

```c
struct dirent *mapping_de;
```
## Detailed Description
This function handles the replay of XLOG_HEAP2_REWRITE WAL records during crash recovery or standby replay. When a table is rewritten (such as during VACUUM FULL or ALTER TABLE), PostgreSQL needs to maintain mapping information for logical decoding to correctly map old tuple identifiers to new ones. This function reconstructs the logical rewrite mapping files from the WAL record data.

The function creates or reopens a mapping file in the pg_logical/mappings directory, truncates it to the specified offset to ensure consistency, writes the mapping data from the WAL record, and syncs the file to disk. The mapping file uses a specific naming format that includes the database OID, relation OID, starting LSN, transaction ID, and current transaction ID.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record data to be replayed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData: Extract data from WAL record
  - XLogRecGetXid: Get transaction ID from WAL record
  - [OpenTransientFile](../O/OpenTransientFile.md): Open file with automatic cleanup
  - [CloseTransientFile](../C/CloseTransientFile.md): Close transient file
  - ftruncate: Truncate file to specified size
  - [pg_pwrite](../p/pg_pwrite.md): Positioned write to file
  - [pg_fsync](../p/pg_fsync.md): Sync file to disk
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end: Report wait events for monitoring
  - [data_sync_elevel](../d/data_sync_elevel.md): Get error level for data sync operations
- Called from (representative examples):
  - [heap2_redo](heap2_redo.md): Main heap WAL record replay function

## Notes and Other Information
- Creates mapping files under pg_logical/mappings/ with format including database, relation, LSN, and transaction IDs
- Uses wait events (WAIT_EVENT_LOGICAL_REWRITE_*) for proper monitoring of I/O operations
- Ensures data durability through explicit truncation, writing, and fsync operations
- Part of the logical decoding infrastructure that maintains tuple mapping consistency across table rewrites
- Critical for maintaining logical replication continuity when tables undergo structural changes

## Simplified Source

```c
void
heap_xlog_logical_rewrite(XLogReaderState *r)
{
    char path[MAXPGPATH];
    int fd;
    xl_heap_rewrite_mapping *xlrec;
    uint32 len;
    char *data;

    // Extract WAL record data
    xlrec = (xl_heap_rewrite_mapping *) XLogRecGetData(r);

    // Generate mapping file path using record information
    snprintf(path, MAXPGPATH,
             "pg_logical/mappings/" LOGICAL_REWRITE_FORMAT,
             xlrec->mapped_db, xlrec->mapped_rel,
             LSN_FORMAT_ARGS(xlrec->start_lsn),
             xlrec->mapped_xid, XLogRecGetXid(r));

    // Open mapping file (create if doesn't exist)
    fd = OpenTransientFile(path, O_CREAT | O_WRONLY | PG_BINARY);
    if (fd < 0)
        ereport(ERROR, (errmsg("could not create file \"%s\": %m", path)));

    // Truncate file to ensure consistency (remove any stale data)
    pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE);
    if (ftruncate(fd, xlrec->offset) != 0)
        ereport(ERROR, (errmsg("could not truncate file \"%s\" to %u: %m",
                              path, (uint32) xlrec->offset)));
    pgstat_report_wait_end();

    // Get mapping data from WAL record
    data = XLogRecGetData(r) + sizeof(*xlrec);
    len = xlrec->num_mappings * sizeof(LogicalRewriteMappingData);

    // Write mapping data to file
    pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE);
    if (pg_pwrite(fd, data, len, xlrec->offset) != len) {
        if (errno == 0)
            errno = ENOSPC;
        ereport(ERROR, (errmsg("could not write to file \"%s\": %m", path)));
    }
    pgstat_report_wait_end();

    // Sync file to disk for durability
    pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC);
    if (pg_fsync(fd) != 0)
        ereport(data_sync_elevel(ERROR),
                (errmsg("could not fsync file \"%s\": %m", path)));
    pgstat_report_wait_end();

    // Close the file
    if (CloseTransientFile(fd) != 0)
        ereport(ERROR, (errmsg("could not close file \"%s\": %m", path)));
}
```