# ginxlogInsert

## Location
[src/include/access/ginxlog.h:55-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginxlog.h#L55-L61)

## Overview
ginxlogInsert is a WAL (Write-Ahead Logging) record structure used to log insertion operations in PostgreSQL's GIN (Generalized Inverted Index) access method, serving as the common header for all GIN insertion log records.

## Definition


## Detailed Description
The ginxlogInsert structure serves as the header for XLOG_GIN_INSERT WAL records in PostgreSQL's GIN index implementation. This structure provides a common format for logging various types of insertion operations in GIN indexes, including both entry tree and posting tree insertions. The structure is designed to be followed by additional data that varies depending on the type of page and operation being logged.

The record format is flexible and includes variable-length data that follows the fixed header. For non-leaf pages, it includes block numbers of child pages involved in split completion. For the actual insertion data, it includes either a ginxlogInsertEntry structure (for entry insertions) or ginxlogRecompressDataLeaf structure (for data page operations), depending on the page type being modified.

## Parameters / Member Variables
- : A 16-bit field containing bitwise flags that describe the type of insertion operation:
  - GIN_INSERT_ISDATA (0x01): Indicates this is a data page insertion
  - GIN_INSERT_ISLEAF (0x02): Indicates this is a leaf page insertion

## Dependencies
- Functions called/Symbols referenced:
  - [IndexTupleData](../I/IndexTupleData.md) (through associated structures)
  - [BlockIdData](../B/BlockIdData.md) (for child page references)
  - [ginxlogInsertEntry](ginxlogInsertEntry.md) (as following data)
  - ginxlogRecompressDataLeaf (as following data)

- Called from (representative examples):
  - [ginPlaceToPage](ginPlaceToPage.md) (src/backend/access/gin/ginbtree.c:421, 426)
  - [ginRedoInsert](ginRedoInsert.md) (src/backend/access/gin/ginxlog.c:350, 364) 
  - [gin_desc](gin_desc.md) (src/backend/access/rmgrdesc/gindesc.c:84, 91)

## Notes and Other Information
- The structure is designed with careful attention to memory alignment, requiring that following structures be only 16-bit aligned
- This is the XLOG record type associated with XLOG_GIN_INSERT (0x20) operations
- The variable-length data that follows depends on the flags set and the type of operation
- Used in WAL replay during crash recovery to recreate GIN index insertion operations
- Critical for maintaining data consistency and durability in GIN indexes