# reconstruct_from_incremental_file

## Location
src/bin/pg_combinebackup/reconstruct.c: 88 - 382

## Overview
Reconstructs a full file from an incremental backup file by traversing a chain of prior backups to gather all necessary blocks.

## Definition


## Detailed Description
This function is the core of PostgreSQL's incremental backup reconstruction process. It takes an incremental backup file and combines it with blocks from a chain of prior backups to create a complete, reconstructed file. The function implements an intelligent block-sourcing strategy where it first processes the latest incremental file, then traverses backwards through the backup chain to find missing blocks.

The reconstruction process handles both incremental and full files in the backup chain. When a full file is found, it can either be copied entirely (if no blocks from later incrementals are needed) or serve as a source for missing blocks. The function also manages checksum validation and can reuse existing checksums from backup manifests when available.

## Parameters / Member Variables
- : Path to the incremental file to be reconstructed
- : Path where the reconstructed full file will be written
- : Directory path relative to backup root, must end with trailing slash
- : Filename without the "INCREMENTAL." prefix
- : Number of previous backups in the chain
- : Array of pathnames to prior backup directories
- : Array of manifest data structures for checksum validation
- : Path to the manifest file for checksum lookup
- : Type of checksum to calculate for the reconstructed file
- : Output parameter for calculated checksum length
- : Output parameter for calculated checksum data
- : Method to use for file copying operations
- : Flag to enable debug output during reconstruction
- : Flag to perform reconstruction without actually writing files

## Dependencies
- Functions called/Symbols referenced:
  - make_incremental_rfile
  - find_reconstructed_block_length
  - make_rfile
  - copy_file
  - write_reconstructed_file
  - debug_reconstruction
  - pg_checksum_init
  - pg_checksum_final
  - manifest_files_lookup
- Called from (representative examples):
  - process_directory_recursively

## Notes and Other Information
The function implements sophisticated optimization logic, including the ability to perform full file copies when no blocks from later incrementals are needed. It handles zero-filled blocks that may not be present in any backup due to PostgreSQL's WAL-based incremental backup strategy. The function also includes comprehensive error handling for cases where the backup chain is incomplete or inconsistent.