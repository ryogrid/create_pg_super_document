# reconstruct_from_incremental_file

## Location
[src/bin/pg_combinebackup/reconstruct.c:88-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L88-L382)

## Overview
Reconstructs a full file from an incremental backup file by traversing a chain of prior backups to gather all necessary blocks.

## Definition

```c
void
reconstruct_from_incremental_file(char *input_filename,
								  char *output_filename,
								  char *relative_path,
								  char *bare_file_name,
								  int n_prior_backups,
								  char **prior_backup_dirs,
								  manifest_data **manifests,
								  char *manifest_path,
								  pg_checksum_type checksum_type,
								  int *checksum_length,
								  uint8 **checksum_payload,
								  CopyMethod copy_method,
								  bool debug,
								  bool dry_run)
```
## Detailed Description
This function is the core of PostgreSQL's incremental backup reconstruction process. It takes an incremental backup file and combines it with blocks from a chain of prior backups to create a complete, reconstructed file. The function implements an intelligent block-sourcing strategy where it first processes the latest incremental file, then traverses backwards through the backup chain to find missing blocks.

The reconstruction process handles both incremental and full files in the backup chain. When a full file is found, it can either be copied entirely (if no blocks from later incrementals are needed) or serve as a source for missing blocks. The function also manages checksum validation and can reuse existing checksums from backup manifests when available.

## Parameters / Member Variables
- `*input_filename`: Path to the incremental file to be reconstructed
- `*output_filename`: Path where the reconstructed full file will be written
- `*relative_path`: Directory path relative to backup root, must end with trailing slash
- `*bare_file_name`: Filename without the "INCREMENTAL." prefix
- `n_prior_backups`: Number of previous backups in the chain
- `**prior_backup_dirs`: Array of pathnames to prior backup directories
- `**manifests`: Array of manifest data structures for checksum validation
- `*manifest_path`: Path to the manifest file for checksum lookup
- `checksum_type`: Type of checksum to calculate for the reconstructed file
- `*checksum_length`: Output parameter for calculated checksum length
- `**checksum_payload`: Output parameter for calculated checksum data
- `copy_method`: Method to use for file copying operations
- `debug`: Flag to enable debug output during reconstruction
- `dry_run`: Flag to perform reconstruction without actually writing files
## Dependencies
- Functions called/Symbols referenced:
  - [make_incremental_rfile](../m/make_incremental_rfile.md)
  - [find_reconstructed_block_length](../f/find_reconstructed_block_length.md)
  - [make_rfile](../m/make_rfile.md)
  - [copy_file](../c/copy_file.md)
  - [write_reconstructed_file](../w/write_reconstructed_file.md)
  - [debug_reconstruction](../d/debug_reconstruction.md)
  - [pg_checksum_init](../p/pg_checksum_init.md)
  - [pg_checksum_final](../p/pg_checksum_final.md)
  - manifest_files_lookup
- Called from (representative examples):
  - [process_directory_recursively](../p/process_directory_recursively.md)

## Notes and Other Information
The function implements sophisticated optimization logic, including the ability to perform full file copies when no blocks from later incrementals are needed. It handles zero-filled blocks that may not be present in any backup due to PostgreSQL's WAL-based incremental backup strategy. The function also includes comprehensive error handling for cases where the backup chain is incomplete or inconsistent.