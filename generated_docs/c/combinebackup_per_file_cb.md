# combinebackup_per_file_cb

## Location
[src/bin/pg_combinebackup/load_manifest.c:268-292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/load_manifest.c#L268-L292)

## Overview
A callback function that processes individual file entries from backup manifests, storing file metadata (path, size, checksum information) in a hash table for pg_combinebackup operations.

## Definition

```c
struct describing this WAL range. */
	range = palloc(sizeof(manifest_wal_range));
```
## Detailed Description
This function serves as a per-file callback during JSON manifest parsing, processing each file entry found in the backup manifest. It creates a new entry in the manifest's hash table for efficient file lookup and stores all relevant file metadata including the pathname, file size, checksum type, and checksum data.

The function enforces uniqueness by checking for duplicate pathnames and terminating with a fatal error if a duplicate is encountered. Each file entry becomes a manifest_file structure containing all necessary information for subsequent backup validation and combination operations.

## Parameters / Member Variables
- `context`: Pointer to the JSON manifest parse context containing private_data with manifest_data structure
- `pathname`: The file path as stored in the backup manifest
- `size`: The file size in bytes
- `checksum_type`: The type of checksum used for this file (enum pg_checksum_type)
- `checksum_length`: The length of the checksum data in bytes
- `checksum_payload`: Pointer to the actual checksum data bytes

## Dependencies
- Functions called/Symbols referenced:
  - manifest_files_insert
  - [pg_fatal](../p/pg_fatal.md) (when duplicate pathname is found)
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md) (type reference)
  - [manifest_data](../m/manifest_data.md) (type reference)
  - [manifest_file](../m/manifest_file.md) (type reference)
  - pg_checksum_type (enum type)
- Called from:
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:147) - set as per_file_cb callback
  - Referenced in SH_DEFINE macro context

## Notes and Other Information
- Function is declared static, limiting scope to load_manifest.c
- Enforces pathname uniqueness within a single backup manifest - duplicate paths cause fatal errors
- Stores checksum payload pointer directly without copying - relies on parser memory management
- Critical component for building the file inventory used in backup combination operations
- Designed as a callback function for the JSON manifest parser infrastructure
- Creates manifest_file entries that are later used for file validation and reconstruction
- Part of the manifest parsing callback system that builds the comprehensive file database for pg_combinebackup

## Simplified Source

```c
static void combinebackup_per_file_cb(JsonManifestParseContext *context,
                                     const char *pathname, size_t size,
                                     pg_checksum_type checksum_type,
                                     int checksum_length, uint8 *checksum_payload) {
    manifest_data *manifest = context->private_data;
    manifest_file *m;
    bool found;

    // Add file entry to hash table
    m = manifest_files_insert(manifest->files, pathname, &found);
    if (found)
        pg_fatal("duplicate path name in backup manifest: \"%s\"", pathname);

    // Store file metadata
    m->size = size;
    m->checksum_type = checksum_type;
    m->checksum_length = checksum_length;
    m->checksum_payload = checksum_payload;
}
```