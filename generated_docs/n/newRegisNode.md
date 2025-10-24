# newRegisNode

## Location
[src/backend/tsearch/regis.c:74-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/regis.c#L74-L84)

## Overview
Creates and initializes a new RegisNode structure for building linked lists of regex pattern nodes in PostgreSQL's text search system.

## Definition

```c
static RegisNode *
newRegisNode(RegisNode *prev, int len)
```
## Detailed Description
newRegisNode is a utility function that allocates memory for a new RegisNode structure and optionally links it to a previous node in a linked list. It allocates memory using palloc0 (zero-initialized allocation) with space for the RegisNode header plus additional length for storing pattern data. If a previous node is provided, it updates the previous node's next pointer to maintain the linked list chain.

## Parameters / Member Variables
- `*prev`: Pointer to the previous RegisNode in the linked list (can be NULL for first node)
- `len`: Additional length of data to allocate beyond the RegisNode header size
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (zero-initialized memory allocation)
  - [RegisNode](../R/RegisNode.md) (structure type)
  - RNHDRSZ (RegisNode header size constant)
- Called from:
  - [RS_compile](../R/RS_compile.md) (multiple times at lines 102, 104, 112, 114)

## Notes and Other Information
- Static function, only accessible within regis.c
- Uses zero-initialized allocation (palloc0) to ensure clean memory state
- Automatically maintains linked list integrity by linking to previous node
- Memory allocation size is RNHDRSZ + len + 1 to accommodate header, data, and null terminator
- Part of the regex compilation infrastructure for text search

## Simplified Source

```c
static RegisNode *
newRegisNode(RegisNode *prev, int len)
{
    // Allocate zero-initialized memory for node header plus data
    RegisNode *ptr = (RegisNode *) palloc0(RNHDRSZ + len + 1);

    // Link to previous node if provided
    if (prev)
        prev->next = ptr;

    return ptr;
}
```