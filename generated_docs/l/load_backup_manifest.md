# load_backup_manifest

## Location
[src/bin/pg_combinebackup/load_manifest.c:105-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/load_manifest.c#L105-L227)

## Overview
Parses the backup_manifest file in the named backup directory and constructs a hash table with information about all the files it mentions, along with a linked list of all the WAL ranges it mentions.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
This function loads and parses a PostgreSQL backup manifest file ("backup_manifest") located in the specified backup directory. The manifest contains metadata about all files in the backup and WAL ranges. The function creates a hash table to efficiently store file information and initializes callback functions for parsing different sections of the JSON manifest.

The function handles both small manifests (loaded entirely into memory) and large manifests (parsed incrementally in chunks) to efficiently manage memory usage. If the backup_manifest file doesn't exist, it logs a warning and returns NULL rather than failing fatally.

## Parameters / Member Variables
- `backup_directory`: The directory path containing the backup_manifest file to be loaded and parsed

## Dependencies
- Functions called/Symbols referenced:
  - open, fstat, read, close (system calls)
  - manifest_files_create
  - [pg_malloc0](../p/pg_malloc0.md), pg_malloc, pfree
  - [json_parse_manifest](../j/json_parse_manifest.md)
  - [json_parse_manifest_incremental_init](../j/json_parse_manifest_incremental_init.md)
  - [json_parse_manifest_incremental_chunk](../j/json_parse_manifest_incremental_chunk.md)  
  - [json_parse_manifest_incremental_shutdown](../j/json_parse_manifest_incremental_shutdown.md)
  - [combinebackup_version_cb](../c/combinebackup_version_cb.md)
  - [combinebackup_system_identifier_cb](../c/combinebackup_system_identifier_cb.md)
  - [combinebackup_per_file_cb](../c/combinebackup_per_file_cb.md)
  - [combinebackup_per_wal_range_cb](../c/combinebackup_per_wal_range_cb.md)
  - [report_manifest_error](../r/report_manifest_error.md)
- Called from:
  - load_backup_manifests (src/bin/pg_combinebackup/load_manifest.c:90)
  - [main](../m/main.md) function in pg_combinebackup via load_backup_manifests

## Notes and Other Information
- Returns NULL if the backup_manifest file doesn't exist (logs warning)
- Estimates initial hash table size based on manifest file size using ESTIMATED_BYTES_PER_MANIFEST_LINE
- Uses READ_CHUNK_SIZE for incremental parsing of large manifest files
- Handles chunked reading intelligently to ensure the last chunk contains the complete checksum portion
- Sets up comprehensive callback functions for different JSON manifest elements during parsing
- Memory management includes proper cleanup of buffers and incremental parsing state

## Simplified Source

```c
manifest_data *load_backup_manifest(char *backup_directory) {
    char pathname[MAXPGPATH];
    int fd;
    struct stat statbuf;
    manifest_data *result;

    // Open manifest file
    snprintf(pathname, MAXPGPATH, "%s/backup_manifest", backup_directory);
    if ((fd = open(pathname, O_RDONLY | PG_BINARY, 0)) < 0) {
        if (errno == ENOENT) {
            pg_log_warning("file \"%s\" does not exist", pathname);
            return NULL;
        }
        pg_fatal("could not open file \"%s\": %m", pathname);
    }

    // Get file size for parsing strategy
    if (fstat(fd, &statbuf) != 0)
        pg_fatal("could not stat file \"%s\": %m", pathname);

    // Create hash table and result structure
    off_t estimate = statbuf.st_size / ESTIMATED_BYTES_PER_MANIFEST_LINE;
    uint32 initial_size = Min(PG_UINT32_MAX, Max(estimate, 256));
    manifest_files_hash *ht = manifest_files_create(initial_size, NULL);

    result = pg_malloc0(sizeof(manifest_data));
    result->files = ht;

    // Setup JSON parsing callbacks
    JsonManifestParseContext context;
    context.private_data = result;
    context.version_cb = combinebackup_version_cb;
    context.system_identifier_cb = combinebackup_system_identifier_cb;
    context.per_file_cb = combinebackup_per_file_cb;
    context.per_wal_range_cb = combinebackup_per_wal_range_cb;
    context.error_cb = report_manifest_error;

    // Parse file (small files: all at once, large files: chunked)
    if (statbuf.st_size <= READ_CHUNK_SIZE) {
        // Parse entire file at once
        char *buffer = pg_malloc(statbuf.st_size);
        if (read(fd, buffer, statbuf.st_size) != statbuf.st_size)
            pg_fatal("could not read file \"%s\"", pathname);

        close(fd);
        json_parse_manifest(&context, buffer, statbuf.st_size);
        pfree(buffer);
    } else {
        // Parse incrementally for large files
        JsonManifestParseIncrementalState *inc_state =
            json_parse_manifest_incremental_init(&context);
        char *buffer = pg_malloc(READ_CHUNK_SIZE + 1);
        int bytes_left = statbuf.st_size;

        while (bytes_left > 0) {
            int bytes_to_read = Min(READ_CHUNK_SIZE, bytes_left);
            if (read(fd, buffer, bytes_to_read) != bytes_to_read)
                pg_fatal("could not read file \"%s\"", pathname);

            bytes_left -= bytes_to_read;
            json_parse_manifest_incremental_chunk(inc_state, buffer,
                                                 bytes_to_read, bytes_left == 0);
        }

        json_parse_manifest_incremental_shutdown(inc_state);
        close(fd);
        pfree(buffer);
    }

    return result;
}
```