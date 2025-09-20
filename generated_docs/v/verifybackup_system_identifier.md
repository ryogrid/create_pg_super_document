# verifybackup_system_identifier

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:535-547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L535-L547)

## Overview
Callback function used during manifest parsing to extract and store the PostgreSQL system identifier from the backup manifest for later validation.

## Definition

```c
static void
verifybackup_system_identifier(JsonManifestParseContext *context,
							   uint64 manifest_system_identifier)
```
## Detailed Description
The verifybackup_system_identifier function serves as a callback function for the JSON manifest parser specifically for handling the system identifier information found in backup manifest files. When the parser encounters the system identifier field in the manifest, it calls this function to record the 64-bit system identifier value in the manifest_data structure.

The system identifier is a unique identifier for a PostgreSQL database cluster that helps ensure backup integrity and prevents restoration of backups to incompatible database systems. This function extracts the private_data from the parsing context and stores the system identifier for later validation during the backup verification process.

## Parameters / Member Variables
- : Pointer to JsonManifestParseContext containing parsing state and private data
- : 64-bit unsigned integer representing the PostgreSQL system identifier extracted from the JSON manifest

## Dependencies
- Functions called/Symbols referenced:
  - [JsonManifestParseContext](../J/JsonManifestParseContext.md) (parsing context structure)
  - [manifest_data](../m/manifest_data.md) (structure for storing manifest information)
- Called from:
  - [parse_manifest_file](../p/parse_manifest_file.md) (in src/bin/pg_verifybackup/pg_verifybackup.c:423)

## Notes and Other Information
- Located in src/bin/pg_verifybackup/pg_verifybackup.c:535-547
- Part of the callback system used by the JSON manifest parser
- Defers actual validation of the system identifier to a later stage in the verification process
- The function simply stores the system identifier without performing any immediate validation
- Works in conjunction with other callback functions like verifybackup_version_cb, verifybackup_per_file_cb, and verifybackup_per_wal_range_cb
- The stored system identifier is crucial for ensuring backup compatibility and preventing mismatched restorations
- System identifiers are generated when a PostgreSQL cluster is initialized and remain constant throughout the cluster's lifetime