# GetIncrementalFileSize

## Location
src/backend/backup/basebackup_incremental.c: 899 - 920

## Overview
Computes the total size for a complete incremental backup file containing a specified number of blocks, including both header and data sections.

## Definition
size_t GetIncrementalFileSize(unsigned num_blocks_required)

## Detailed Description
This function calculates the total file size needed for an incremental backup file that will store a given number of data blocks. The calculation includes both the header size (computed by GetIncrementalHeaderSize) and the actual block data storage. The header contains metadata including magic number, truncation block length, block count, and block number arrays, while the data section stores the actual block contents.

## Parameters / Member Variables
- num_blocks_required: The number of blocks that the incremental file will contain

## Dependencies
- Functions called/Symbols referenced:
  - [GetIncrementalHeaderSize](GetIncrementalHeaderSize.md) (computes header size)
  - Assert (macro for assertion checking)
  - RELSEG_SIZE (constant defining maximum blocks per segment)
  - BLCKSZ (constant defining block size)
- Called from (representative examples):
  - [sendDir](../s/sendDir.md)

## Notes and Other Information
- The function includes overflow protection by asserting that num_blocks_required doesn't exceed RELSEG_SIZE
- Total file size = header size + (BLCKSZ * number of blocks)
- This is a key component in PostgreSQL's incremental backup size estimation
- Used during backup operations to determine storage requirements before file creation