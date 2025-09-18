# debug_reconstruction

## Location
[src/bin/pg_combinebackup/reconstruct.c:383-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L383-L437)

## Overview
Performs post-reconstruction logging and sanity checks to validate the file reconstruction process and provide debug information.

## Definition


## Detailed Description
This function is called after file reconstruction to provide debug logging and perform validation checks. It iterates through all source files that were involved in the reconstruction process, logging the number of blocks read from each source file. In dry-run mode, it performs additional validation by checking that each source file is actually long enough to satisfy the read operations that would have been performed during reconstruction.

The function serves both debugging and validation purposes, ensuring that the reconstruction process would succeed in a real run when operating in dry-run mode, and providing detailed information about which files contributed blocks to the final reconstructed file.

## Parameters / Member Variables
- `n_source`: Number of source files in the sources array
- `sources`: Array of rfile pointers representing all potential source files
- `dry_run`: Flag indicating whether this was a dry-run reconstruction

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug
  - fstat
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [reconstruct_from_incremental_file](../r/reconstruct_from_incremental_file.md)

## Notes and Other Information
This function only processes sources that were actually used (non-NULL and had blocks read). In dry-run mode, it performs file size validation to ensure the source files contain enough data to satisfy the reconstruction requirements. The function helps identify potential issues in backup chains before attempting actual reconstruction.