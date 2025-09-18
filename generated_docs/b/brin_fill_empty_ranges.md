# brin_fill_empty_ranges

## Location
src/backend/access/brin/brin.c: 2980 - 3004

## Overview
Adds BRIN index tuples representing empty page ranges between specified boundaries during index construction or maintenance.

## Definition


## Detailed Description
The  function is responsible for filling gaps in BRIN index coverage by adding empty summary tuples for page ranges that don't have any existing index entries. This function is called during BRIN index construction and parallel merge operations to ensure complete coverage of the table's block ranges.

The function operates by iterating through page ranges between the specified boundaries (exclusive) and creating empty BRIN tuples for each range. It uses a pre-built empty tuple template that is reused across all insertions for efficiency. The empty tuples serve as placeholders that can later be updated when actual data is inserted into those page ranges.

This mechanism ensures that the BRIN index maintains complete coverage of the table's address space, which is essential for proper index functionality and query planning.

## Parameters / Member Variables
- : Pointer to BrinBuildState structure containing the current state of BRIN index construction, including the relation being indexed, pages per range configuration, and the pre-built empty tuple template
- : The block number of the last processed page range (exclusive boundary). If InvalidBlockNumber, indicates this is the first range and processing should start from block 0
- : The block number where empty range filling should stop (exclusive boundary). Only ranges with starting block numbers less than this value will be processed

## Dependencies
- Functions called/Symbols referenced:
  - BrinBuildState (struct type)
  - brin_build_empty_tuple
  - brin_doinsert
- Called from (representative examples):
  - brinbuild
  - _brin_parallel_merge
  - BRIN_ALL_BLOCKRANGES (macro context)

## Notes and Other Information
- The function is declared as static, meaning it's only accessible within the brin.c source file
- Empty tuples are built only once per build state and then reused for all subsequent insertions, optimizing performance
- The function handles the special case where prevRange is InvalidBlockNumber by starting from block 0
- Page ranges are incremented by state->bs_pagesPerRange, which defines the granularity of BRIN index coverage
- This function is critical for maintaining BRIN index integrity and ensuring complete coverage of table address space during both initial index construction and parallel merge operations