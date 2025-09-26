# smgrtruncate

## Location
[src/backend/storage/smgr/smgr.c:701-726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L701-L726)

## Overview
Truncates the specified forks of a supplied relation to the given number of blocks, serving as a backward-compatible wrapper around the more comprehensive  function.

## Definition

```c
void
smgrtruncate(SMgrRelation reln, ForkNumber *forknum, int nforks,
			 BlockNumber *nblocks)
```
## Detailed Description
The  function provides a backward-compatible interface for truncating multiple forks of a storage manager relation. It automatically retrieves the current number of blocks for each fork using  and then delegates the actual truncation operation to . This function is designed for external callers and is not used in PostgreSQL core code. It cannot be used within a critical section due to its dependency on  which may perform I/O operations.

## Parameters / Member Variables
- : SMgrRelation pointer representing the storage manager relation to truncate
- : Array of ForkNumber values indicating which forks to truncate
- : Integer specifying the number of forks in the forknum array
- : Array of BlockNumber values specifying the target size for each corresponding fork

## Dependencies
- Functions called/Symbols referenced:
  - SMgrRelation (type)
  - MAX_FORKNUM (constant)
  - smgrnblocks
  - smgrtruncate2
- Called from (representative examples):
  - SmgrIsTemp

## Notes and Other Information
- This is a backward-compatible version of  for external callers
- Not used in PostgreSQL core code, which directly uses 
- Cannot be used in a critical section due to potential I/O operations in 
- Automatically determines the current block count for each fork before truncation
- The function creates a local  array to store current block counts for each fork
- Located in src/backend/storage/smgr/smgr.c:701-726