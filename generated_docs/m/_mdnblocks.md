# _mdnblocks

## Location
[src/backend/storage/smgr/md.c:1727-1747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L1727-L1747)

## Overview
Get the number of blocks present in a single disk file segment for a PostgreSQL relation.

## Definition

```c
static BlockNumber
_mdnblocks(SMgrRelation reln, ForkNumber forknum, MdfdVec *seg)
```
## Detailed Description
This is a static function that determines the number of blocks present in a specific segment of a relation's disk file. It works by getting the file size using FileSize() and dividing by the block size (BLCKSZ). The function handles error conditions when the file size cannot be determined and includes a note that any partial block at the end of file (EOF) will be ignored in the calculation.

The function is part of PostgreSQL's magnetic disk storage manager (md.c) and is used internally by other functions to determine the current size of relation segments.

## Parameters / Member Variables
- : SMgrRelation pointer representing the storage manager relation
- : ForkNumber indicating which fork of the relation (main, FSM, VM, etc.)
- : MdfdVec pointer to the specific segment whose block count is needed

## Dependencies
- Functions called/Symbols referenced:
  - FileSize (to get the file size in bytes)
  - [FilePathName](../F/FilePathName.md) (for error reporting to get the file path name)
  - ereport (for error reporting)
  - [errcode_for_file_access](../e/errcode_for_file_access.md) (for error code generation)
  - [errmsg](../e/errmsg.md) (for error message formatting)
- Called from (representative examples):
  - [mdnblocks](mdnblocks.md) (main public interface for getting block count)
  - mdextend (when extending files)
  - mdzeroextend (when zero-extending files)
  - mdopenfork (when opening relation forks)
  - [_mdfd_openseg](_mdfd_openseg.md) (when opening segments)
  - [_mdfd_getseg](_mdfd_getseg.md) (when getting segments)

## Notes and Other Information
- This is a static function, so it's only accessible within md.c
- The calculation ignores partial blocks at EOF, meaning the returned count represents only complete blocks
- Error handling is robust - if FileSize() fails, an appropriate error is reported with the file path
- The function is fundamental to PostgreSQL's block-oriented storage system
- Returns BlockNumber type, which is typically a 32-bit unsigned integer representing block numbers