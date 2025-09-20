# TablespaceList

## Location
[src/bin/pg_basebackup/pg_basebackup.c:50-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L50-L54)

## Overview
A container structure that manages a linked list of tablespace directory mappings using head and tail pointers for efficient list operations.

## Definition

```c
typedef struct TablespaceList
{
	TablespaceListCell *head;
	TablespaceListCell *tail;
} TablespaceList;
```
## Detailed Description
TablespaceList is a list management structure in pg_basebackup that provides an efficient way to maintain a collection of tablespace directory mappings. It implements a standard linked list container pattern with both head and tail pointers, allowing for O(1) insertion at both ends of the list and efficient traversal from the beginning.

This structure works in conjunction with TablespaceListCell to provide a complete tablespace mapping system. The head pointer allows for easy traversal of all tablespace mappings, while the tail pointer enables efficient append operations when new tablespace mappings are added during command-line parsing or configuration.

## Parameters / Member Variables
- `*head`: Pointer to the first TablespaceListCell in the linked list, or NULL if the list is empty
- `*tail`: Pointer to the last TablespaceListCell in the linked list, or NULL if the list is empty
## Dependencies
- Functions called/Symbols referenced:
  - [TablespaceListCell](TablespaceListCell.md) (referenced by both head and tail pointers)
- Called from (representative examples):
  - CompressionLocation (appears to be used in context with compression settings)

## Notes and Other Information
- This structure provides O(1) append operations through the tail pointer
- Both head and tail pointers will be NULL for an empty list
- The structure enables efficient traversal starting from the head pointer
- Used specifically in pg_basebackup for managing tablespace directory remapping during backup operations
- The dual-pointer design (head and tail) is a common pattern for implementing efficient list operations
- Memory management and list manipulation functions work with this structure to maintain the linked list integrity