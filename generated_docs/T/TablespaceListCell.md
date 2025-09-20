# TablespaceListCell

## Location
[src/bin/pg_basebackup/pg_basebackup.c:43-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L43-L48)

## Overview
A linked list node structure used in pg_basebackup to store tablespace directory mapping information during base backup operations.

## Definition

```c
typedef struct TablespaceListCell
{
	struct TablespaceListCell *next;
	char		old_dir[MAXPGPATH];
	char		new_dir[MAXPGPATH];
} TablespaceListCell;
```
## Detailed Description
TablespaceListCell is a fundamental data structure in pg_basebackup that represents a single node in a linked list used to manage tablespace directory mappings. This structure is essential for handling tablespace relocation during base backup operations, allowing users to specify different destination directories for tablespaces compared to their original locations on the source server.

The structure implements a simple singly-linked list pattern where each node contains the mapping information for one tablespace and a pointer to the next node in the list. This design allows for dynamic allocation and management of tablespace mappings without requiring a fixed-size array.

## Parameters / Member Variables
- : Pointer to the next TablespaceListCell in the linked list, or NULL if this is the last node
- : Original tablespace directory path on the source server (maximum MAXPGPATH characters)
- : Target tablespace directory path on the destination system (maximum MAXPGPATH characters)

## Dependencies
- Functions called/Symbols referenced:
  - (Self-referential through next pointer)
- Called from (representative examples):
  - [TablespaceList](TablespaceList.md) (typedef for pointer to this structure)
  - [tablespace_list_append](../t/tablespace_list_append.md)
  - [get_tablespace_mapping](../g/get_tablespace_mapping.md)

## Notes and Other Information
- This structure is specifically used in pg_basebackup utility for managing tablespace directory remapping
- The MAXPGPATH constant defines the maximum length for PostgreSQL path names
- The linked list design allows for an arbitrary number of tablespace mappings to be specified
- Memory management for these structures is handled by the functions that manipulate the tablespace list
- This is part of the tablespace mapping functionality that allows users to relocate tablespaces during backup restoration