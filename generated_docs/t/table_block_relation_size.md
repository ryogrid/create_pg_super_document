# table_block_relation_size

## Location
src/backend/access/table/tableam.c: 617 - 653

## Overview
A helper function that calculates the total size in bytes of a relation by counting blocks and multiplying by the block size, supporting both individual forks and all forks combined.

## Definition
```c
uint64 table_block_relation_size(Relation rel, ForkNumber forkNumber)
```

## Detailed Description
This function provides a standard implementation of relation size calculation for table access methods that store data in the standard PostgreSQL relation fork structure. It leverages the storage manager (smgr) interface to count blocks and converts the result to bytes by multiplying by BLCKSZ.

The function handles two modes of operation: when a specific fork number is provided, it returns the size of that fork only; when InvalidForkNumber is passed, it iterates through all possible forks (up to MAX_FORKNUM) and returns the total size across all forks.

This is a convenience function that table access methods can use directly rather than implementing their own relation_size callback, provided they follow PostgreSQL's standard fork-based storage model.

## Parameters / Member Variables
- `rel`: The relation whose size is to be calculated
- `forkNumber`: The specific fork to measure, or InvalidForkNumber to measure all forks

## Dependencies
- Functions called/Symbols referenced:
  - smgrnblocks
  - RelationGetSmgr
  - InvalidForkNumber
  - MAX_FORKNUM
  - BLCKSZ
- Called from (representative examples):
  - SampleHeapTupleVisible
  - table_scan_sample_next_tuple

## Notes and Other Information
- Returns size in bytes, not blocks
- Designed as a reusable helper for table access methods following standard fork conventions
- Handles the special case of InvalidForkNumber to sum across all forks
- Relies on the storage manager layer for the actual block counting
- Part of the table access method framework introduced in PostgreSQL 12+