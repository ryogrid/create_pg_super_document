# backup_manifest_option

## Location
src/include/backup/backup_manifest.h: 25 - 26

## Overview
The backup_manifest_option enumeration defines the available options for controlling backup manifest generation during PostgreSQL base backup operations.

## Definition

```c
typedef struct backup_manifest_info
{
	BufFile    *buffile;
	pg_checksum_type checksum_type;
	pg_cryptohash_ctx *manifest_ctx;
	uint64		manifest_size;
	bool		force_encode;
	bool		first_file;
	bool		still_checksumming;
} backup_manifest_info;
```
## Detailed Description
The backup_manifest_option enumeration provides three distinct modes for controlling how backup manifests are generated during base backup operations. This enumeration allows users to specify whether they want a manifest generated, whether to skip manifest generation entirely, or whether to enable special encoding behavior for the manifest content. The option directly influences the initialization and behavior of the backup_manifest_info structure, particularly determining whether manifest processing occurs and how content is encoded.

## Parameters / Member Variables
- : Enable standard manifest generation with normal encoding behavior
- : Disable manifest generation entirely; no manifest file will be created
- : Enable manifest generation with forced base64 encoding of file paths and content for safe text transmission

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references - this is an enumeration type)
- Called from (representative examples):
  - InitializeBackupManifest
  - SINK_BUFFER_LENGTH (basebackup.c context)

## Notes and Other Information
- MANIFEST_OPTION_NO results in manifest->buffile being set to NULL during initialization, effectively disabling all manifest processing
- MANIFEST_OPTION_FORCE_ENCODE is useful when the manifest needs to be transmitted over protocols that may not handle binary or special characters safely
- The force encoding option primarily affects how file paths and other potentially problematic content is represented in the JSON manifest
- This enumeration is part of the backup manifest API introduced to provide fine-grained control over backup documentation and verification capabilities
- The enum values are designed to be easily testable with simple equality comparisons in conditional logic