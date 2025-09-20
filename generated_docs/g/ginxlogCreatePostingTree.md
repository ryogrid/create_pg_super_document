# ginxlogCreatePostingTree

## Location
[src/include/access/ginxlog.h:21-25](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginxlog.h#L21-L25)

## Overview
Structure used for WAL (Write-Ahead Logging) record when creating a new posting tree in a GIN index data page.

## Definition

```c
typedef struct ginxlogCreatePostingTree
{
	uint32		size;
	/* A compressed posting list follows */
} ginxlogCreatePostingTree;
```
## Detailed Description
The ginxlogCreatePostingTree structure is used as part of WAL logging when creating a new posting tree in GIN (Generalized Inverted Index) indexes. This structure stores metadata about the compressed posting list that will be written to the WAL record. The actual compressed posting list data follows immediately after this structure in the WAL record. This mechanism ensures that posting tree creation operations can be properly replayed during recovery scenarios.

## Parameters / Member Variables
- : Size in bytes of the compressed posting list that follows this structure in the WAL record

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this structure)
- Called from (representative examples):
  - [createPostingTree](../c/createPostingTree.md) (in src/backend/access/gin/gindatapage.c:1842,1847)
  - [ginRedoCreatePTree](ginRedoCreatePTree.md) (in src/backend/access/gin/ginxlog.c:47,57)

## Notes and Other Information
- This structure is part of the GIN index WAL logging infrastructure (XLOG_GIN_CREATE_PTREE operation)
- The compressed posting list data immediately follows this structure in memory/WAL record
- Used both during normal operation (createPostingTree) and during WAL replay (ginRedoCreatePTree)
- Defined in src/include/access/ginxlog.h:21-25