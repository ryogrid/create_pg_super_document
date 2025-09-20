# cb_options

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:70-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L70-L81)

## Overview
A structure that stores all command-line options and configuration parameters for the pg_combinebackup utility.

## Definition

```c
typedef struct cb_options
{
	bool		debug;
	char	   *output;
	bool		dry_run;
	bool		no_sync;
	cb_tablespace_mapping *tsmappings;
	pg_checksum_type manifest_checksums;
	bool		no_manifest;
	DataDirSyncMethod sync_method;
	CopyMethod	copy_method;
} cb_options;
```
## Detailed Description
The  structure serves as the central configuration container for pg_combinebackup, storing all user-specified options and operational parameters. This structure is populated during command-line parsing and is passed throughout the application to control behavior during backup combination operations.

The structure encapsulates various aspects of the combination process including debugging settings, output configuration, synchronization behavior, tablespace mappings, checksum handling, and file copy methods. This centralized approach ensures consistent access to configuration parameters across all phases of the backup combination process.

## Parameters / Member Variables
- `debug`: Enable debug output and verbose logging
- `*output`: Path to the output directory where the combined backup will be created
- `dry_run`: Perform a dry run without actually creating files (validation only)
- `no_sync`: Skip filesystem synchronization operations for faster completion
- `*tsmappings`: Linked list of tablespace directory mappings for relocation
- `manifest_checksums`: Type of checksums to use in backup manifests
- `no_manifest`: Skip manifest file generation during combination
- `sync_method`: Method to use for data directory synchronization
- `copy_method`: Method to use for file copying operations
## Dependencies
- Functions called/Symbols referenced:
  - [debug](../d/debug.md) (boolean type)
  - [cb_tablespace_mapping](cb_tablespace_mapping.md) (structure type)
  - pg_checksum_type (enumeration)
  - [DataDirSyncMethod](../D/DataDirSyncMethod.md) (enumeration)
  - [CopyMethod](../C/CopyMethod.md) (enumeration)
- Called from (representative examples):
  - [main](../m/main.md)
  - [add_tablespace_mapping](../a/add_tablespace_mapping.md)
  - create_output_directory
  - process_directory_recursively
  - [scan_for_existing_tablespaces](../s/scan_for_existing_tablespaces.md)

## Notes and Other Information
- Central configuration structure for pg_combinebackup utility
- Populated during command-line argument parsing phase
- Passed to most functions to provide consistent access to user preferences
- Contains both operational flags (debug, dry_run, no_sync) and functional parameters (output path, copy methods)
- The tsmappings member provides access to the tablespace relocation functionality
- Essential for coordinating behavior across all phases of backup combination