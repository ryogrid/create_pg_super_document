# _IndexList

## Location
[src/backend/bootstrap/bootstrap.c:163-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L163-L168)

## Overview
The  struct (typedef'd as ) is used during PostgreSQL's bootstrap phase to queue index creation operations for later execution.

## Definition

```c
typedef struct _IndexList
{
	Oid			il_heap;
	Oid			il_ind;
	IndexInfo  *il_info;
	struct _IndexList *il_next;
} IndexList;
```
## Detailed Description
The  structure implements a linked list that stores information about indexes that need to be built during the bootstrap process. During PostgreSQL initialization, indexes are first declared but not immediately built. Instead, their metadata is stored in this linked list structure, and the actual index building is deferred until all catalog tables are properly initialized.

This two-phase approach (declare first, build later) is necessary because indexes themselves have catalog entries that need to be included in the indexes on catalog tables. The structure allows the bootstrap process to collect all index declarations and then build them in the correct order once the system catalogs are ready.

## Parameters / Member Variables
- `il_heap`: OID of the heap table that the index is built on
- `il_ind`: OID of the index relation itself
- `*il_info`: Pointer to IndexInfo structure containing detailed index metadata (columns, expressions, predicates, etc.)
- `*il_next`: Pointer to the next IndexList node in the linked list
## Dependencies
- Functions called/Symbols referenced:
  - IndexInfo (structure for index metadata)
  - struct _IndexList (self-reference for linked list)
- Called from (representative examples):
  - [index_register](../i/index_register.md) (creates new IndexList nodes at line 921)
  - [build_indices](../b/build_indices.md) (traverses the list at line 953)

## Notes and Other Information
The linked list is managed through a static global variable  that points to the head of the list. New index declarations are added to the front of the list (LIFO order). Memory allocation for IndexList nodes uses a special no-GC memory context () to ensure the index information persists throughout the bootstrap process. This structure is only used during bootstrap and becomes obsolete once the database initialization is complete.