# DataPageDeleteStack

## Location
[src/backend/access/gin/ginvacuum.c:114-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L114-L123)

## Overview
DataPageDeleteStack is a doubly-linked list structure used during GIN index vacuum operations to track and manage the hierarchical deletion of data pages in posting trees.

## Definition

```c
typedef struct DataPageDeleteStack
{
	struct DataPageDeleteStack *child;
	struct DataPageDeleteStack *parent;

	BlockNumber blkno;			/* current block number */
	Buffer		leftBuffer;		/* pinned and locked rightest non-deleted page
								 * on left */
	bool		isRoot;
} DataPageDeleteStack;
```
## Detailed Description
The DataPageDeleteStack structure implements a stack-like data structure that maintains parent-child relationships during GIN posting tree traversal for page deletion. It serves as a navigation aid when scanning through posting tree levels to identify pages that can be safely deleted during vacuum operations. Each stack entry represents a level in the posting tree hierarchy, with the structure maintaining both upward (parent) and downward (child) links to facilitate tree traversal. The structure also tracks buffer management information to ensure proper locking and pinning of pages during the deletion process.

## Parameters / Member Variables
- `*child`: Pointer to the child level DataPageDeleteStack entry (one level deeper in the tree)
- `*parent`: Pointer to the parent level DataPageDeleteStack entry (one level higher in the tree)
- `blkno`: Block number of the current page being processed at this level
- `leftBuffer`: Buffer containing the rightmost non-deleted page to the left of the current position, kept pinned and locked for reference
- `isRoot`: Boolean flag indicating whether this stack entry represents the root level of the posting tree
## Dependencies
- Functions called/Symbols referenced:
  - [DataPageDeleteStack](DataPageDeleteStack.md) (self-references for linked list structure)
- Called from (representative examples):
  - [ginScanToDelete](../g/ginScanToDelete.md)
  - [ginVacuumPostingTree](../g/ginVacuumPostingTree.md)

## Notes and Other Information
This structure is critical for maintaining state during depth-first traversal of GIN posting trees during vacuum operations. The doubly-linked design allows for efficient backtracking when moving between tree levels. The leftBuffer field is particularly important for maintaining proper page ordering and ensuring that page deletions don't break the tree structure. The stack-based approach enables the vacuum process to safely navigate complex posting tree hierarchies while keeping track of deletion candidates at multiple levels simultaneously.