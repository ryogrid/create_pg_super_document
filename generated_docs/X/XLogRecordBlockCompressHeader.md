# XLogRecordBlockCompressHeader

## Location
src/include/access/xlogrecord.h: 173 - 176

## Overview
XLogRecordBlockCompressHeader provides additional metadata required when a page image is both compressed and contains a "hole", storing the hole length information needed for proper page reconstruction.

## Definition


## Detailed Description
XLogRecordBlockCompressHeader is a specialized header structure used only when a page image undergoes both hole removal and compression optimizations. When a page image is compressed, the original hole length information cannot be derived by simple arithmetic (subtracting stored bytes from BLCKSZ), making it necessary to explicitly store the hole length. This structure provides that missing piece of information, enabling proper reconstruction of the original page during WAL replay. The header is only included when both BKPIMAGE_HAS_HOLE and BKPIMAGE_COMPRESSED flags are set.

## Parameters / Member Variables
- : The exact number of bytes that were removed from the middle of the page as the "hole" during WAL storage optimization

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)

- Called from (representative examples):
  - [XLogRecordAssemble](XLogRecordAssemble.md)
  - SizeOfXLogRecordBlockCompressHeader

## Notes and Other Information
- This header only appears when both hole removal and compression are applied to a page image
- Required because compression makes it impossible to calculate hole length from stored data size alone
- Forms part of the variable-length header chain: XLogRecordBlockHeader → XLogRecordBlockImageHeader → XLogRecordBlockCompressHeader
- Essential for accurate page reconstruction during crash recovery and replication
- Represents the intersection of two major WAL optimization techniques in PostgreSQL
- The structure is kept minimal with only essential information to reduce WAL overhead