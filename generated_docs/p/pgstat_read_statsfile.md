# pgstat_read_statsfile

## Location
[src/backend/utils/activity/pgstat.c:1493-1693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1493-L1693)

## Overview
The  function reads an existing statistics file from disk and loads all statistics data into the shared memory hash table, restoring previously persisted statistics after a PostgreSQL restart.

## Definition

```c
struct
	 */
	if (!read_chunk_s(fpin, &shmem->archiver.stats))
		goto error;
```
## Detailed Description
This function is responsible for reading the permanent statistics file and populating the shared statistics hash table during PostgreSQL startup or statistics system initialization. It reads both fixed statistics structures (like archiver, bgwriter, checkpointer, IO, SLRU, and WAL stats) and variable statistics entries (identified by hash keys or names).

The function implements a robust file format validation and error handling mechanism. If the statistics file doesn't exist (common on first startup or after stats collection was disabled), the function gracefully returns, allowing PostgreSQL to start with empty statistics. For corrupted files or other errors, it calls  to ensure a clean state.

The reading process follows a specific file format:
1. Format ID validation
2. Fixed statistics structures (archiver, bgwriter, checkpointer, IO, SLRU, WAL)
3. Variable entries marked with 'S' (normal entries) or 'N' (named entries like slots)
4. End marker 'E'

## Parameters / Member Variables
This function takes no parameters as it operates on global state and predefined file paths.

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateFile](../A/AllocateFile.md): Opens the statistics file for reading
  - read_chunk_s/read_chunk: Reads structured data from the file
  - [pgstat_reset_after_failure](pgstat_reset_after_failure.md): Called when file reading fails
  - [dshash_find_or_insert](../d/dshash_find_or_insert.md): Inserts statistics entries into shared hash table
  - [dshash_release_lock](../d/dshash_release_lock.md): Releases locks on hash table entries
  - [pgstat_init_entry](pgstat_init_entry.md): Initializes a new statistics entry
  - [pgstat_get_kind_info](pgstat_get_kind_info.md): Gets metadata for statistics entry types
  - [pgstat_get_entry_len](pgstat_get_entry_len.md)/pgstat_get_entry_data: Accesses entry size and data
  - [FreeFile](../F/FreeFile.md): Closes the file handle
  - unlink: Removes the statistics file after successful reading

- Called from (representative examples):
  - [pgstat_restore_stats](pgstat_restore_stats.md): Main function that orchestrates statistics restoration

## Notes and Other Information
- This function must only be called from a single process accessing shared stats (no locking required)
- Should not be called from the postmaster process
- The function removes the permanent statistics file after successful reading to prevent reprocessing
- Implements comprehensive error handling with detailed logging
- Supports both hash-key identified entries and name-identified entries (e.g., replication slots)
- File format validation prevents corruption from causing system instability
- Located in src/backend/utils/activity/pgstat.c:1493-1693

## Simplified Source

```c
// Simplified version of pgstat_read_statsfile
static void
pgstat_read_statsfile(void)
{
    FILE       *file_handle;
    int32       format_id;
    bool        entry_found;
    const char *stats_filename = PGSTAT_STAT_PERMANENT_FILENAME;
    PgStat_ShmemControl *shared_memory = pgStatLocal.shmem;

    // Validate execution context - shouldn't be called from postmaster
    Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

    // Try to open the statistics file
    if ((file_handle = AllocateFile(stats_filename, PG_BINARY_R)) == NULL) {
        if (errno != ENOENT) {
            // Log error if file exists but can't be opened
            ereport(LOG, (errcode_for_file_access(),
                         errmsg("could not open statistics file \"%s\": %m", stats_filename)));
        }
        // Reset statistics state and return gracefully
        pgstat_reset_after_failure();
        return;
    }

    // Verify file format version
    if (!read_chunk_s(file_handle, &format_id) || format_id != PGSTAT_FILE_FORMAT_ID) {
        goto error_cleanup;
    }

    // Read fixed statistics structures in sequence
    if (!read_chunk_s(file_handle, &shared_memory->archiver.stats) ||
        !read_chunk_s(file_handle, &shared_memory->bgwriter.stats) ||
        !read_chunk_s(file_handle, &shared_memory->checkpointer.stats) ||
        !read_chunk_s(file_handle, &shared_memory->io.stats) ||
        !read_chunk_s(file_handle, &shared_memory->slru.stats) ||
        !read_chunk_s(file_handle, &shared_memory->wal.stats)) {
        goto error_cleanup;
    }

    // Read variable statistics entries until end marker
    for (;;) {
        int entry_type = fgetc(file_handle);

        switch (entry_type) {
            case 'S':  // Standard entry identified by hash key
            case 'N':  // Named entry (e.g., replication slots)
                {
                    PgStat_HashKey key;
                    PgStatShared_HashEntry *hash_entry;
                    PgStatShared_Common *entry_header;

                    // Read entry key (different methods for 'S' vs 'N')
                    if (entry_type == 'S') {
                        if (!read_chunk_s(file_handle, &key) || !pgstat_is_kind_valid(key.kind)) {
                            goto error_cleanup;
                        }
                    } else {
                        // Handle named entries with validation
                        PgStat_Kind kind;
                        NameData name;
                        const PgStat_KindInfo *kind_info;

                        if (!read_chunk_s(file_handle, &kind) ||
                            !read_chunk_s(file_handle, &name) ||
                            !pgstat_is_kind_valid(kind)) {
                            goto error_cleanup;
                        }

                        kind_info = pgstat_get_kind_info(kind);
                        if (!kind_info->from_serialized_name ||
                            !kind_info->from_serialized_name(&name, &key)) {
                            // Skip unknown entries
                            fseek(file_handle, pgstat_get_entry_len(kind), SEEK_CUR);
                            continue;
                        }
                    }

                    // Insert entry into shared hash table
                    hash_entry = dshash_find_or_insert(pgStatLocal.shared_hash, &key, &entry_found);

                    if (entry_found) {
                        // Duplicate entries indicate corruption
                        dshash_release_lock(pgStatLocal.shared_hash, hash_entry);
                        elog(WARNING, "found duplicate stats entry %d/%u/%u",
                             key.kind, key.dboid, key.objoid);
                        goto error_cleanup;
                    }

                    // Initialize and populate entry data
                    entry_header = pgstat_init_entry(key.kind, hash_entry);
                    dshash_release_lock(pgStatLocal.shared_hash, hash_entry);

                    if (!read_chunk(file_handle,
                                   pgstat_get_entry_data(key.kind, entry_header),
                                   pgstat_get_entry_len(key.kind))) {
                        goto error_cleanup;
                    }
                    break;
                }

            case 'E':  // End of file marker
                if (fgetc(file_handle) != EOF) {
                    goto error_cleanup;
                }
                goto success_cleanup;

            default:
                goto error_cleanup;
        }
    }

success_cleanup:
    FreeFile(file_handle);
    unlink(stats_filename);  // Remove file after successful read
    return;

error_cleanup:
    ereport(LOG, (errmsg("corrupted statistics file \"%s\"", stats_filename)));
    pgstat_reset_after_failure();
    FreeFile(file_handle);
}
```

Key simplifications made:
- Consolidated repetitive structure reading into single conditional check
- Simplified variable names for better readability (file_handle, shared_memory, etc.)
- Grouped related error conditions to reduce goto usage
- Added descriptive comments explaining each major section
- Combined similar validation checks into single expressions
- Abstracted complex nested conditions into clearer flow
- Removed detailed debug logging for brevity while preserving error logging