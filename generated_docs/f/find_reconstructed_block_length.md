# find_reconstructed_block_length

## Location
src/bin/pg_combinebackup/reconstruct.c: 438 - 454

## Overview
Calculates the required block length for a reconstructed file based on the truncation length and the highest block number present in an incremental backup.

## Definition


## Detailed Description
This function determines how many blocks the reconstructed output file should contain. It starts with the truncation_block_length from the incremental file (the minimum length the file should have) and then examines all blocks present in the incremental file. If any block has a number equal to or greater than the initial block length, the function extends the required length to include that block.

This ensures that the reconstructed file will be large enough to contain both the originally truncated content and any additional blocks that were added in the incremental backup beyond the original truncation point.

## Parameters / Member Variables
- `s`: Pointer to rfile structure representing the incremental backup file

## Dependencies
- Functions called/Symbols referenced:
  - rfile (structure access)
- Called from (representative examples):
  - reconstruct_from_incremental_file

## Notes and Other Information
The function implements a simple algorithm that ensures the reconstructed file will accommodate all blocks present in the incremental backup. It handles cases where incremental backups contain blocks beyond the original file's truncation point, which can happen when a file is extended after the base backup was taken.