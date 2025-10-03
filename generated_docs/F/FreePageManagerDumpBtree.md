# FreePageManagerDumpBtree

## Location
[src/backend/utils/mmgr/freepage.c:1250-1295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L1250-L1295)

## Overview
A recursive debugging function that generates a hierarchical dump of the B-tree structure used by the PostgreSQL Free Page Manager for managing available memory spans.

## Definition

```c
static void
FreePageManagerDumpBtree(FreePageManager *fpm, FreePageBtree *btp,
						 FreePageBtree *parent, int level, StringInfo buf)
```
## Detailed Description
This function performs a depth-first traversal of the Free Page Manager's B-tree structure, generating a formatted textual representation of the tree's contents and structure. It serves as a debugging aid by displaying page numbers, tree levels, node types (internal vs leaf), parent-child relationships, and key information for each node in the tree. The function validates parent pointers and recursively processes child nodes for internal nodes.

## Parameters / Member Variables
- `*fpm`: Pointer to the FreePageManager instance containing the B-tree
- `*btp`: Pointer to the current FreePageBtree node being processed
- `*parent`: Expected parent node pointer for validation purposes
- `level`: Current depth level in the tree (used for formatting)
- `buf`: StringInfo buffer to append the formatted dump output
## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - fpm_pointer_to_page
  - [check_stack_depth](../c/check_stack_depth.md)
  - relptr_access
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [FreePageManagerDumpBtree](FreePageManagerDumpBtree.md) (recursive call)
- Called from (representative examples):
  - [FreePageManagerDump](FreePageManagerDump.md)
  - [FreePageManagerDumpBtree](FreePageManagerDumpBtree.md) (recursive)

## Notes and Other Information
- This is a static function used only for debugging purposes
- Validates parent-child relationships and reports discrepancies
- Distinguishes between internal nodes (marked 'i') and leaf nodes (marked 'l') using magic numbers
- Internal nodes display first_page->child_page mappings
- Leaf nodes display first_page(npages) information
- Uses check_stack_depth() to prevent stack overflow during deep recursions