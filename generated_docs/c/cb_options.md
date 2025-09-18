# cb_options

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 70 - 81

## Overview
A structure that stores all command-line options and configuration parameters for the pg_combinebackup utility.

## Definition


## Detailed Description
The  structure serves as the central configuration container for pg_combinebackup, storing all user-specified options and operational parameters. This structure is populated during command-line parsing and is passed throughout the application to control behavior during backup combination operations.

The structure encapsulates various aspects of the combination process including debugging settings, output configuration, synchronization behavior, tablespace mappings, checksum handling, and file copy methods. This centralized approach ensures consistent access to configuration parameters across all phases of the backup combination process.

## Parameters / Member Variables
- : Enable debug output and verbose logging
- : Path to the output directory where the combined backup will be created
- : Perform a dry run without actually creating files (validation only)
- : Skip filesystem synchronization operations for faster completion
- : Linked list of tablespace directory mappings for relocation
- : Type of checksums to use in backup manifests
- : Skip manifest file generation during combination
- : Method to use for data directory synchronization
- : Method to use for file copying operations

## Dependencies
- Functions called/Symbols referenced:
  - debug (boolean type)
  - cb_tablespace_mapping (structure type)
  - pg_checksum_type (enumeration)
  - DataDirSyncMethod (enumeration)
  - CopyMethod (enumeration)
- Called from (representative examples):
  - main
  - add_tablespace_mapping
  - create_output_directory
  - process_directory_recursively
  - scan_for_existing_tablespaces

## Notes and Other Information
- Central configuration structure for pg_combinebackup utility
- Populated during command-line argument parsing phase
- Passed to most functions to provide consistent access to user preferences
- Contains both operational flags (debug, dry_run, no_sync) and functional parameters (output path, copy methods)
- The tsmappings member provides access to the tablespace relocation functionality
- Essential for coordinating behavior across all phases of backup combination