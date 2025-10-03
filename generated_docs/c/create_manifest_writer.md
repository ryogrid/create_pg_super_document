# create_manifest_writer

## Location
[src/bin/pg_combinebackup/write_manifest.c:48-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/write_manifest.c#L48-L75)

## Overview
Creates and initializes a new backup manifest writer for generating PostgreSQL backup manifest files in JSON format.

## Definition

```c
manifest_writer *
create_manifest_writer(char *directory, uint64 system_identifier)
```
## Detailed Description
This function creates a new manifest writer structure used for generating backup manifest files during PostgreSQL backup operations. It initializes the writer with a target directory and system identifier, sets up internal buffers and checksumming, and begins the JSON structure for the backup manifest. The manifest file will be created as "backup_manifest" in the specified directory and follows the PostgreSQL Backup Manifest format version 2.

The function performs several initialization tasks:
- Allocates memory for the manifest writer structure
- Constructs the full pathname for the backup manifest file
- Initializes string buffers for building the JSON content
- Sets up SHA256 checksumming for the manifest file
- Begins the JSON structure with version information and system identifier

## Parameters / Member Variables
- `*directory`: Target directory path where the backup_manifest file will be created
- `system_identifier`: PostgreSQL system identifier to include in the manifest metadata
## Dependencies
- Functions called/Symbols referenced:
  - [manifest_writer](../m/manifest_writer.md) (structure type)
  - [pg_malloc](../p/pg_malloc.md) (memory allocation)
  - [pg_checksum_init](../p/pg_checksum_init.md) (checksum initialization)
  - CHECKSUM_TYPE_SHA256 (checksum algorithm constant)
  - UINT64_FORMAT (format specifier for uint64)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_combinebackup/pg_combinebackup.c:333)

## Notes and Other Information
- The manifest writer structure maintains state for incremental building of the JSON manifest
- The function initializes checksumming to ensure manifest integrity
- The backup manifest format follows PostgreSQL Backup Manifest specification version 2
- File descriptor is initially set to -1 and will be opened when writing begins
- The JSON structure is started but not completed by this function - additional functions handle file entries and finalization