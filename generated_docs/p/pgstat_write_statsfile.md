# pgstat_write_statsfile

## Location
[src/backend/utils/activity/pgstat.c:1310-1478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1310-L1478)

## Overview
Writes all PostgreSQL statistics data to a persistent file on disk, typically called during server shutdown to preserve statistics across restarts.

## Definition

```c
struct
	 */
	pgstat_build_snapshot_fixed(PGSTAT_KIND_ARCHIVER);
```
## Detailed Description
This function is responsible for persisting the entire PostgreSQL statistics subsystem state to disk. It writes statistics data to a temporary file first, then atomically renames it to the permanent location to ensure data integrity. The function handles both fixed-format statistics (like archiver, bgwriter, checkpointer stats) and variable entries (databases, tables, functions, etc.) from the shared hash table.

The writing process includes:
1. Opening a temporary statistics file
2. Writing a format identifier header
3. Writing all fixed-format statistics snapshots for different subsystems
4. Iterating through the shared hash table to write all dynamic statistics entries
5. Closing the file and atomically renaming it to the permanent location

The function uses deferred error checking - individual write operations don't check for errors immediately, but  is called at the end to detect any write failures. This approach is more efficient for bulk write operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - : Verifies statistics subsystem is operational
  - : Opens the temporary statistics file
  -  and : Helper functions for writing data
  - : Builds snapshots for fixed statistics kinds
  - : Gets metadata about statistics kinds
  - : Iterates through shared hash table entries
  - : Gets statistics entry data and size
  - File system functions: , , 

- Called from (representative examples):
  - : During server shutdown process

## Notes and Other Information
- This function is called only during server shutdown when no locking is required
- Uses atomic file replacement (write to temp, then rename) for data safety
- Writes both fixed-format statistics and dynamic hash table entries
- Includes comprehensive error handling with detailed logging
- The function sets  to NONE during shutdown
- Statistics entries marked as 'dropped' are skipped during writing
- The file format includes type indicators ('S' for standard entries, 'N' for named entries, 'E' for end)
- Comments suggest the function could be generalized to iterate over  instead of hardcoding statistics types

## Simplified Source

```c
// Simplified version of pgstat_write_statsfile
static void
pgstat_write_statsfile(void)
{
    FILE *fpout;
    int32 format_id;
    const char *tmpfile = PGSTAT_STAT_PERMANENT_TMPFILE;
    const char *statfile = PGSTAT_STAT_PERMANENT_FILENAME;
    dshash_seq_status hstat;
    PgStatShared_HashEntry *ps;

    // Ensure stats system is up and disable consistency checking during shutdown
    pgstat_assert_is_up();
    pgstat_fetch_consistency = PGSTAT_FETCH_CONSISTENCY_NONE;

    // Open temporary file for writing statistics
    fpout = AllocateFile(tmpfile, PG_BINARY_W);
    if (fpout == NULL) {
        ereport(LOG, (errmsg("could not open temporary statistics file")));
        return;
    }

    // Write file format header
    format_id = PGSTAT_FILE_FORMAT_ID;
    write_chunk_s(fpout, &format_id);

    // Write all fixed-format statistics structures
    // (archiver, bgwriter, checkpointer, IO, SLRU, WAL)
    pgstat_build_snapshot_fixed(PGSTAT_KIND_ARCHIVER);
    write_chunk_s(fpout, &pgStatLocal.snapshot.archiver);

    pgstat_build_snapshot_fixed(PGSTAT_KIND_BGWRITER);
    write_chunk_s(fpout, &pgStatLocal.snapshot.bgwriter);

    pgstat_build_snapshot_fixed(PGSTAT_KIND_CHECKPOINTER);
    write_chunk_s(fpout, &pgStatLocal.snapshot.checkpointer);

    pgstat_build_snapshot_fixed(PGSTAT_KIND_IO);
    write_chunk_s(fpout, &pgStatLocal.snapshot.io);

    pgstat_build_snapshot_fixed(PGSTAT_KIND_SLRU);
    write_chunk_s(fpout, &pgStatLocal.snapshot.slru);

    pgstat_build_snapshot_fixed(PGSTAT_KIND_WAL);
    write_chunk_s(fpout, &pgStatLocal.snapshot.wal);

    // Write all dynamic statistics entries from shared hash table
    dshash_seq_init(&hstat, pgStatLocal.shared_hash, false);
    while ((ps = dshash_seq_next(&hstat)) != NULL) {
        PgStatShared_Common *shstats;
        const PgStat_KindInfo *kind_info;

        // Skip dropped entries (should not happen during shutdown)
        if (ps->dropped)
            continue;

        shstats = (PgStatShared_Common *) dsa_get_address(pgStatLocal.dsa, ps->body);
        kind_info = pgstat_get_kind_info(ps->key.kind);

        // Write entry type marker and key/name
        if (!kind_info->to_serialized_name) {
            // Standard entry identified by hash key
            fputc('S', fpout);
            write_chunk_s(fpout, &ps->key);
        } else {
            // Named entry (e.g., replication slots)
            NameData name;
            kind_info->to_serialized_name(&ps->key, shstats, &name);
            fputc('N', fpout);
            write_chunk_s(fpout, &ps->key.kind);
            write_chunk_s(fpout, &name);
        }

        // Write the actual statistics data
        write_chunk(fpout,
                    pgstat_get_entry_data(ps->key.kind, shstats),
                    pgstat_get_entry_len(ps->key.kind));
    }
    dshash_seq_term(&hstat);

    // Mark end of file and handle completion/errors
    fputc('E', fpout);

    if (ferror(fpout) || FreeFile(fpout) < 0) {
        ereport(LOG, (errmsg("could not write statistics file")));
        unlink(tmpfile);
    } else if (rename(tmpfile, statfile) < 0) {
        ereport(LOG, (errmsg("could not rename statistics file")));
        unlink(tmpfile);
    }
}
```

Key simplifications made:
- Removed detailed error handling messages for clarity
- Consolidated similar fixed-format statistics writing into clear pattern
- Simplified error conditions and cleanup logic
- Removed detailed comments about shutdown state and assertions
- Focused on the main execution path: write header, write fixed stats, write dynamic stats, finalize file
- Abstracted complex error reporting to simple log messages