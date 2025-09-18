# transfer_relfile

## Location
src/bin/pg_upgrade/relfilenumber.c: 176 - 266

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