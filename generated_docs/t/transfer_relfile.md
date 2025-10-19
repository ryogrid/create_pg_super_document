# transfer_relfile

## Location
[src/bin/pg_upgrade/relfilenumber.c:176-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/relfilenumber.c#L176-L266)

## Overview
This static function performs the actual file transfer operation for individual relation files, handling segmented files and special visibility map processing during PostgreSQL cluster upgrades.

## Definition
```c
static void transfer_relfile(FileNameMap *map, const char *type_suffix, bool vm_must_add_frozenbit)
```

## Detailed Description
The `transfer_relfile` function is responsible for transferring individual relation files from the old PostgreSQL cluster to the new cluster. It handles PostgreSQL's file segmentation where large files are broken into 1GB segments (named relfilenumber, relfilenumber.1, relfilenumber.2, etc.). The function processes all segments of a relation file and supports different transfer modes including cloning, copying (with or without copy_file_range), and hard linking.

A key feature is its handling of visibility map files when upgrading from older PostgreSQL versions that lack the frozen bit feature. In such cases, visibility map files are rewritten using `rewriteVisibilityMap` regardless of the transfer mode to ensure compatibility with the new cluster format.

The function includes error handling for missing files (which is acceptable for auxiliary files like _fsm and _vm) and empty files, providing appropriate logging throughout the transfer process.

## Parameters / Member Variables
- `map`: FileNameMap structure containing source and destination file information including paths, OIDs, and relation names
- `type_suffix`: String suffix indicating the file type ("" for main file, "_fsm" for free space map, "_vm" for visibility map)
- `vm_must_add_frozenbit`: Boolean flag indicating whether visibility map files need to be rewritten to add frozen bit support

## Dependencies
- Functions called/Symbols referenced:
  - unlink
  - [pg_log](../p/pg_log.md)
  - [rewriteVisibilityMap](../r/rewriteVisibilityMap.md)
  - [cloneFile](../c/cloneFile.md)
  - [copyFile](../c/copyFile.md)
  - [copyFileByRange](../c/copyFileByRange.md)
  - [linkFile](../l/linkFile.md)
  - TRANSFER_MODE_CLONE, TRANSFER_MODE_COPY, TRANSFER_MODE_COPY_FILE_RANGE, TRANSFER_MODE_LINK
  - PG_STATUS, PG_VERBOSE
- Called from (representative examples):
  - [transfer_single_new_db](transfer_single_new_db.md)

## Notes and Other Information
- The function processes all segments of a relation file in a loop, stopping when no more segments exist
- It constructs file paths using tablespace paths, database OID, and relation file number from the mapping
- Empty files and missing auxiliary files are handled gracefully without errors
- The function removes existing destination files before transfer to avoid conflicts
- Visibility map rewriting takes precedence over the configured transfer mode when vm_must_add_frozenbit is true
- Detailed logging is provided at both STATUS and VERBOSE levels for monitoring transfer progress
- The function is marked static, indicating it is only used within the same source file

## Simplified Source

```c
static void transfer_relfile(FileNameMap *map, const char *type_suffix, bool vm_must_add_frozenbit) {
    char old_file[MAXPGPATH];
    char new_file[MAXPGPATH];
    char extent_suffix[65];
    struct stat statbuf;

    // Process all segments of the relation file (1GB segments)
    for (int segno = 0;; segno++) {
        // Build segment suffix (.1, .2, etc. or empty for first segment)
        if (segno == 0)
            extent_suffix[0] = '\0';
        else
            snprintf(extent_suffix, sizeof(extent_suffix), ".%d", segno);

        // Construct full file paths for old and new files
        snprintf(old_file, sizeof(old_file), "%s%s/%u/%u%s%s",
                map->old_tablespace, map->old_tablespace_suffix,
                map->db_oid, map->relfilenumber, type_suffix, extent_suffix);
        snprintf(new_file, sizeof(new_file), "%s%s/%u/%u%s%s",
                map->new_tablespace, map->new_tablespace_suffix,
                map->db_oid, map->relfilenumber, type_suffix, extent_suffix);

        // Check if file exists (for auxiliary files and segments)
        if (type_suffix[0] != '\0' || segno != 0) {
            if (stat(old_file, &statbuf) != 0) {
                if (errno == ENOENT)
                    return;  // File doesn't exist, that's OK
                else
                    pg_fatal("error while checking for file existence \"%s.%s\" (\"%s\" to \"%s\"): %m",
                            map->nspname, map->relname, old_file, new_file);
            }
            if (statbuf.st_size == 0)
                return;  // Empty file, skip
        }

        // Remove existing destination file
        unlink(new_file);
        pg_log(PG_STATUS, "%s", old_file);

        // Handle visibility map rewriting or regular transfer
        if (vm_must_add_frozenbit && strcmp(type_suffix, "_vm") == 0) {
            // Special case: rewrite visibility map for frozen bit support
            pg_log(PG_VERBOSE, "rewriting \"%s\" to \"%s\"", old_file, new_file);
            rewriteVisibilityMap(old_file, new_file, map->nspname, map->relname);
        } else {
            // Regular transfer based on transfer mode
            switch (user_opts.transfer_mode) {
                case TRANSFER_MODE_CLONE:
                    pg_log(PG_VERBOSE, "cloning \"%s\" to \"%s\"", old_file, new_file);
                    cloneFile(old_file, new_file, map->nspname, map->relname);
                    break;
                case TRANSFER_MODE_COPY:
                    pg_log(PG_VERBOSE, "copying \"%s\" to \"%s\"", old_file, new_file);
                    copyFile(old_file, new_file, map->nspname, map->relname);
                    break;
                case TRANSFER_MODE_COPY_FILE_RANGE:
                    pg_log(PG_VERBOSE, "copying \"%s\" to \"%s\" with copy_file_range", old_file, new_file);
                    copyFileByRange(old_file, new_file, map->nspname, map->relname);
                    break;
                case TRANSFER_MODE_LINK:
                    pg_log(PG_VERBOSE, "linking \"%s\" to \"%s\"", old_file, new_file);
                    linkFile(old_file, new_file, map->nspname, map->relname);
            }
        }
    }
}
```