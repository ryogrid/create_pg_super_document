# SimpleOidListCell

## Location
[src/include/fe_utils/simple_list.h:20-24](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/simple_list.h#L20-L24)

## Overview
SimpleOidListCell is a fundamental building block for singly-linked lists that store PostgreSQL Object Identifier (Oid) values, providing a lightweight data structure for frontend utilities.

## Definition

```c
typedef struct SimpleOidListCell
{
	struct SimpleOidListCell *next;
	Oid			val;
} SimpleOidListCell;
```
## Detailed Description
SimpleOidListCell represents a single node in a singly-linked list specifically designed to hold Oid values. This structure is part of PostgreSQL's frontend utility framework and provides an efficient way to create and manipulate lists of object identifiers. The structure follows a standard linked list node pattern with a pointer to the next cell and a data field containing the Oid value. This design allows for dynamic list construction and traversal while maintaining minimal memory overhead.

## Parameters / Member Variables
- `*next`: Pointer to the next SimpleOidListCell in the linked list, or NULL if this is the last cell
- `val`: The Oid value stored in this cell, representing a PostgreSQL object identifier
## Dependencies
- Functions called/Symbols referenced:
  - [SimpleOidListCell](SimpleOidListCell.md) (self-reference for next pointer)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [simple_oid_list_append](../s/simple_oid_list_append.md)
  - [simple_oid_list_member](../s/simple_oid_list_member.md)
  - [simple_oid_list_destroy](../s/simple_oid_list_destroy.md)
  - [SimpleOidList](SimpleOidList.md) (as the cell type for the list structure)

## Notes and Other Information
- This structure is defined in the frontend utilities header, indicating it's primarily used by client-side PostgreSQL tools rather than the backend server
- The structure follows PostgreSQL's naming convention for simple list implementations
- Memory management for these cells is typically handled by the associated list manipulation functions
- The structure is used as the foundation for SimpleOidList, which maintains head and tail pointers to these cells