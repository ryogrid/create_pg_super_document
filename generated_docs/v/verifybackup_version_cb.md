# verifybackup_version_cb

## Location
src/bin/pg_verifybackup/pg_verifybackup.c: 522 - 534

## Overview
Callback function used during manifest parsing to extract and store the backup manifest version number for later validation.

## Definition


## Detailed Description
The verifybackup_version_cb function serves as a callback function for the JSON manifest parser specifically for handling the version information found in backup manifest files. When the parser encounters the version field in the manifest, it calls this function to record the version number in the manifest_data structure.

The function extracts the private_data from the parsing context (which contains the manifest_data structure) and stores the version number for later validation. The actual validation of the version number occurs at a later stage in the verification process, not within this callback.

## Parameters / Member Variables
- : Pointer to JsonManifestParseContext containing parsing state and private data
- : Integer representing the manifest format version number extracted from the JSON

## Dependencies
- Functions called/Symbols referenced:
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md) (parsing context structure)
  - [manifest_data](../m/manifest_data.md) (structure for storing manifest information)
- Called from:
  - [parse_manifest_file](../p/parse_manifest_file.md) (in src/bin/pg_verifybackup/pg_verifybackup.c:422)

## Notes and Other Information
- Located in src/bin/pg_verifybackup/pg_verifybackup.c:522-534
- Part of the callback system used by the JSON manifest parser
- Defers actual validation of the version to a later stage in the verification process
- The function simply stores the version number without performing any immediate validation
- Works in conjunction with other callback functions like verifybackup_system_identifier, verifybackup_per_file_cb, and verifybackup_per_wal_range_cb
- The stored version information is used later in the backup verification process to ensure compatibility