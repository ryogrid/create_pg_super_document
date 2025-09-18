# combinebackup_system_identifier_cb

## Location
src/bin/pg_combinebackup/load_manifest.c: 255 - 267

## Overview
A callback function that extracts and stores the system identifier from a backup manifest during parsing for pg_combinebackup operations.

## Definition


## Detailed Description
This function serves as a callback during JSON manifest parsing to capture the PostgreSQL system identifier from the backup manifest. The system identifier is a unique 64-bit value that identifies a specific PostgreSQL cluster/database system. The function stores this identifier in the manifest_data structure for later validation.

The function defers validation of the system identifier to a later stage in the backup combination process, simply recording the value for subsequent consistency checks across multiple backup manifests.

## Parameters / Member Variables
- `context`: Pointer to the JSON manifest parse context containing private_data with manifest_data structure
- `manifest_system_identifier`: The 64-bit system identifier value extracted from the backup manifest

## Dependencies
- Functions called/Symbols referenced:
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md) (type reference)
  - [manifest_data](../m/manifest_data.md) (type reference and field access)
- Called from:
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:146) - set as system_identifier_cb callback
  - Referenced in SH_DEFINE macro context

## Notes and Other Information
- Function is declared static, limiting scope to load_manifest.c
- Designed as a callback function for the JSON manifest parser infrastructure
- Stores the system identifier without immediate validation - validation occurs later in the backup combination process
- Critical for ensuring all backup manifests belong to the same PostgreSQL cluster
- Accesses the manifest_data structure through the context's private_data field
- Part of the manifest parsing callback system used by pg_combinebackup utility