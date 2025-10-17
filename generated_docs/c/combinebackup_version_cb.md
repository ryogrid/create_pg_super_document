# combinebackup_version_cb

## Location
[src/bin/pg_combinebackup/load_manifest.c:243-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/load_manifest.c#L243-L254)

## Overview
A callback function that validates the backup manifest version number to ensure compatibility with incremental backup operations in pg_combinebackup.

## Definition

```c
static void
combinebackup_version_cb(JsonManifestParseContext *context,
						 int manifest_version)
```
## Detailed Description
This function serves as a version validation callback during manifest parsing for the pg_combinebackup utility. It specifically checks that the manifest version supports incremental backup functionality. The function enforces that only manifest version 2 or later can be used with pg_combinebackup, as version 1 manifests lack the necessary metadata for incremental backup operations.

When an incompatible version (specifically version 1) is encountered, the function terminates the program with a fatal error message explaining that the manifest version does not support incremental backup.

## Parameters / Member Variables
- `context`: Pointer to the JSON manifest parse context (unused in current implementation)
- `manifest_version`: The version number found in the backup manifest file

## Dependencies
- Functions called/Symbols referenced:
  - [pg_fatal](../p/pg_fatal.md) (when version 1 is detected)
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md) (type reference)
- Called from:
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:145) - set as version_cb callback
  - Referenced in SH_DEFINE macro context

## Notes and Other Information  
- Function is declared static, limiting scope to load_manifest.c
- Currently only rejects manifest version 1; version 2 and higher are accepted
- Designed as a callback function for the JSON manifest parser infrastructure
- Critical for ensuring pg_combinebackup only processes compatible backup manifests
- The context parameter is provided for callback interface compatibility but not utilized
- Part of the incremental backup validation system in PostgreSQL's backup tooling

## Simplified Source

```c
static void combinebackup_version_cb(JsonManifestParseContext *context,
                                   int manifest_version) {
    // Check if manifest version supports incremental backup
    if (manifest_version == 1)
        pg_fatal("backup manifest version 1 does not support incremental backup");
}
```