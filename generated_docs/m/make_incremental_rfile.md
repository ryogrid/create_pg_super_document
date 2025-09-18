# make_incremental_rfile

## Location
src/bin/pg_combinebackup/reconstruct.c: 455 - 509

## Overview
Initializes and reads the header of an incremental backup file, creating an rfile structure with metadata about which blocks it contains.

## Definition


## Detailed Description
This function creates an rfile structure for an incremental backup file by reading and parsing its header. The incremental file format includes a magic number for validation, the number of blocks contained in the file, the truncation block length (indicating the original file size), and an array of block numbers that specify which blocks are present in the incremental file.

The function performs validation on the magic number and ensures that block counts and truncation lengths don't exceed PostgreSQL's segment size limits. It also calculates the header length and aligns it to block boundaries to ensure proper data alignment for subsequent block reading operations.

## Parameters / Member Variables
- `filename`: Path to the incremental backup file to initialize

## Dependencies
- Functions called/Symbols referenced:
  - [make_rfile](make_rfile.md)
  - [read_bytes](../r/read_bytes.md)
  - pg_malloc0
  - [pg_fatal](../p/pg_fatal.md)
  - INCREMENTAL_MAGIC
  - RELSEG_SIZE
  - BlockNumber
  - BLCKSZ
- Called from (representative examples):
  - [reconstruct_from_incremental_file](../r/reconstruct_from_incremental_file.md)

## Notes and Other Information
The function validates the incremental file format using a magic number and enforces PostgreSQL's segment size constraints. Header length is aligned to BLCKSZ boundaries only when the file contains actual block data, optimizing for both alignment requirements and storage efficiency. The resulting rfile structure contains all necessary metadata for subsequent block extraction operations during reconstruction.