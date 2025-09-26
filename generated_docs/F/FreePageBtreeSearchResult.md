# FreePageBtreeSearchResult

## Location
[src/backend/utils/mmgr/freepage.c:119-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L119-L125)

## Overview
FreePageBtreeSearchResult is a struct that holds the results of searching a B-tree in the free page manager, containing information about the search position, whether an exact match was found, and resource requirements for potential insertions.

## Definition

```c
typedef struct FreePageBtreeSearchResult
{
	FreePageBtree *page;
	Size		index;
	bool		found;
	unsigned	split_pages;
} FreePageBtreeSearchResult;
```
## Detailed Description
This structure encapsulates the complete result of a B-tree search operation in PostgreSQL's free page management system. It serves as a comprehensive return value that provides not only the search results but also metadata necessary for subsequent operations like insertions or updates. The structure is designed to minimize additional traversals by providing both the exact location information and pre-calculated resource requirements for potential modifications.

The search result indicates either an exact match position or the insertion point where a new key should be placed. Additionally, it calculates the number of B-tree pages that would need to be split if an insertion were to occur at the found position, enabling efficient resource planning for write operations.

## Parameters / Member Variables
- : Pointer to the FreePageBtree page where the search terminated (typically a leaf page)
- : The index position within the page - either the exact match location or the insertion point
- : Boolean flag indicating whether an exact match was found (true) or just an insertion point (false)  
- : Pre-calculated number of additional B-tree pages needed for a split operation during insertion

## Dependencies
- Functions called/Symbols referenced:
  - [FreePageBtree](FreePageBtree.md)
  - Size (PostgreSQL size type)

- Called from (representative examples):
  - [FreePageBtreeSearch](FreePageBtreeSearch.md)
  - [FreePageManagerGetInternal](FreePageManagerGetInternal.md)  
  - [FreePageManagerPutInternal](FreePageManagerPutInternal.md)

## Notes and Other Information
- This structure is used internally by the free page manager's B-tree implementation in src/backend/utils/mmgr/freepage.c
- The split_pages field is crucial for memory allocation planning, as it allows the caller to determine upfront how many pages need to be allocated before attempting an insertion
- The search algorithm traverses from root to leaf, and this structure captures the final state at the leaf level
- When found is false, the index still points to a meaningful location - the position where a new entry should be inserted to maintain B-tree ordering
- The structure helps optimize performance by avoiding re-traversal of the B-tree for insertion operations following a search