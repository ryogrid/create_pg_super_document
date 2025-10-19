# parse_manifest_file

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:390-506](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L390-L506)

## Overview
Parses a PostgreSQL backup manifest file and returns a data structure containing the parsed manifest information including file metadata and validation callbacks.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
The parse_manifest_file function is responsible for reading and parsing a PostgreSQL backup manifest file. It opens the specified manifest file, determines its size, and creates a hash table to store the manifest data. The function supports both single-chunk reading for smaller files and incremental parsing for larger files to handle memory efficiently.

The function sets up a JsonManifestParseContext with appropriate callback functions for handling different parts of the manifest (version, system identifier, per-file data, WAL ranges, and errors). For large files, it uses chunked reading with intelligent chunk sizing to ensure the final chunk contains the complete checksum information.

## Parameters / Member Variables
- : Path to the manifest file to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - open (open the manifest file)
  - [report_fatal_error](../r/report_fatal_error.md) (error reporting for file operations)
  - fstat (get file statistics)
  - manifest_files_create (create hash table for manifest files)
  - [pg_malloc0](pg_malloc0.md) (allocate and zero-initialize memory for result)
  - [verifybackup_version_cb](../v/verifybackup_version_cb.md) (callback for version information)
  - [verifybackup_system_identifier](../v/verifybackup_system_identifier.md) (callback for system identifier)
  - [verifybackup_per_file_cb](../v/verifybackup_per_file_cb.md) (callback for per-file data)
  - [verifybackup_per_wal_range_cb](../v/verifybackup_per_wal_range_cb.md) (callback for WAL range data)
  - [report_manifest_error](../r/report_manifest_error.md) (callback for parsing errors)
  - [pg_malloc](pg_malloc.md) (allocate memory for buffer)
  - read (read file content)
  - close (close file descriptor)
  - [json_parse_manifest](../j/json_parse_manifest.md) (parse manifest in single chunk)
  - [json_parse_manifest_incremental_init](../j/json_parse_manifest_incremental_init.md) (initialize incremental parsing)
  - [json_parse_manifest_incremental_chunk](../j/json_parse_manifest_incremental_chunk.md) (parse manifest chunk)
  - [json_parse_manifest_incremental_shutdown](../j/json_parse_manifest_incremental_shutdown.md) (cleanup incremental parsing)
  - [pfree](pfree.md) (free allocated buffer memory)
- Called from:
  - [main](../m/main.md) (in src/bin/pg_verifybackup/pg_verifybackup.c:345)

## Notes and Other Information
- Located in src/bin/pg_verifybackup/pg_verifybackup.c:390-506
- Uses READ_CHUNK_SIZE constant for chunked reading of large files
- Estimates hash table size based on ESTIMATED_BYTES_PER_MANIFEST_LINE
- Implements intelligent chunking strategy to ensure final chunk is at least half the chunk size to contain complete checksum data
- Returns a dynamically allocated manifest_data structure that must be freed by the caller
- Handles both O(1) single-read parsing for small files and streaming incremental parsing for large files
- Uses PG_BINARY flag for cross-platform compatibility when opening files

## Simplified Source

```c
static manifest_data *parse_manifest_file(char *manifest_path) {
    int fd;
    struct stat statbuf;
    manifest_files_hash *ht;
    char *buffer;
    JsonManifestParseContext context;
    manifest_data *result;
    int chunk_size = READ_CHUNK_SIZE;

    // Open and get size of manifest file
    fd = open(manifest_path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0)
        report_fatal_error("could not open file \"%s\": %m", manifest_path);

    if (fstat(fd, &statbuf) != 0)
        report_fatal_error("could not stat file \"%s\": %m", manifest_path);

    // Create hash table for manifest data
    off_t estimate = statbuf.st_size / ESTIMATED_BYTES_PER_MANIFEST_LINE;
    uint32 initial_size = Min(PG_UINT32_MAX, Max(estimate, 256));
    ht = manifest_files_create(initial_size, NULL);

    // Initialize result structure and parsing context
    result = pg_malloc0(sizeof(manifest_data));
    result->files = ht;
    context.private_data = result;
    context.version_cb = verifybackup_version_cb;
    context.system_identifier_cb = verifybackup_system_identifier;
    context.per_file_cb = verifybackup_per_file_cb;
    context.per_wal_range_cb = verifybackup_per_wal_range_cb;
    context.error_cb = report_manifest_error;

    // Parse file: either all-at-once or in chunks
    if (statbuf.st_size <= chunk_size) {
        // Small file: read entire content and parse
        buffer = pg_malloc(statbuf.st_size);
        int rc = read(fd, buffer, statbuf.st_size);
        if (rc != statbuf.st_size)
            pg_fatal("could not read file \"%s\"", manifest_path);

        close(fd);
        json_parse_manifest(&context, buffer, statbuf.st_size);
    } else {
        // Large file: incremental parsing in chunks
        JsonManifestParseIncrementalState *inc_state;
        inc_state = json_parse_manifest_incremental_init(&context);
        buffer = pg_malloc(chunk_size + 1);

        int bytes_left = statbuf.st_size;
        while (bytes_left > 0) {
            // Determine chunk size (ensure final chunk has complete checksum)
            int bytes_to_read = chunk_size;
            if (bytes_left < chunk_size)
                bytes_to_read = bytes_left;
            else if (bytes_left < 2 * chunk_size)
                bytes_to_read = bytes_left / 2;

            // Read and parse chunk
            int rc = read(fd, buffer, bytes_to_read);
            if (rc != bytes_to_read)
                pg_fatal("could not read file \"%s\"", manifest_path);

            bytes_left -= rc;
            json_parse_manifest_incremental_chunk(inc_state, buffer, rc, bytes_left == 0);
        }

        json_parse_manifest_incremental_shutdown(inc_state);
        close(fd);
    }

    pfree(buffer);
    return result;
}
```