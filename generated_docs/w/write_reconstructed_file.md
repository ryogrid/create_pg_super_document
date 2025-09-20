# write_reconstructed_file

## Location
[src/bin/pg_combinebackup/reconstruct.c:551-750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L551-L750)

## Overview
A core function in the pg_combinebackup utility that reconstructs and writes a complete file from multiple incremental backup sources, handling block-by-block reconstruction with optional dry-run and debugging capabilities.

## Definition

```c
static void
write_reconstructed_file(char *input_filename,
						 char *output_filename,
						 unsigned block_length,
						 rfile **sourcemap,
						 off_t *offsetmap,
						 pg_checksum_context *checksum_ctx,
						 CopyMethod copy_method,
						 bool debug,
						 bool dry_run)
```
## Detailed Description
The  function is the main workhorse for file reconstruction in PostgreSQL's incremental backup system. It takes a mapping of source files and block offsets and reconstructs a complete output file by reading blocks from various sources or zero-filling them when needed.

The function supports multiple copy methods including standard read/write operations and the more efficient  system call when available. It provides comprehensive debugging output showing the reconstruction plan and tracks statistics about blocks read from each source. The function also handles checksum calculation for the reconstructed file and supports a dry-run mode for planning purposes.

## Parameters / Member Variables
- : Name of the input file being processed (used primarily for error messages)
- : Path where the reconstructed file will be written
- : Total number of blocks in the reconstructed file
- : Array mapping each block index to its source rfile structure (NULL for zero-filled blocks)
- : Array of file offsets corresponding to each block's location in its source file
- : Context for checksum calculation during file reconstruction
- : Method to use for copying data (standard copy vs copy_file_range)
- : Flag to enable detailed debugging output showing reconstruction plan
- : Flag to simulate reconstruction without actually creating output file

## Dependencies
- Functions called/Symbols referenced:
  -  (for debugging output)
  -  (for checksum type display)
  - xdg-open - opens a file or URL in the user's preferred application

Synopsis

xdg-open { file | URL }

xdg-open { --help | --manual | --version }

Use 'man xdg-open' or 'xdg-open --manual' for additional info. (to create output file)
  -  (to write individual blocks)
  -  (to read blocks from source files)
  -  (efficient block copying when available)
  -  (for checksum calculation)
  -  (to close output file)

- Called from:
  -  (main reconstruction workflow)

## Notes and Other Information
- This is a static function within the pg_combinebackup reconstruction module
- Supports two copy methods: traditional read/write and copy_file_range for better performance
- Zero-fills blocks that aren't present in any source file (new/uninitialized blocks)
- Provides detailed debugging output showing the reconstruction plan by block ranges
- Tracks statistics for each source file including blocks read and highest offset accessed
- Uses PostgreSQL's standard error reporting with  for unrecoverable errors
- The dry_run mode allows previewing the reconstruction process without file creation
- Handles platform differences gracefully (copy_file_range availability)
- Block size is assumed to be BLCKSZ (typically 8KB in PostgreSQL)