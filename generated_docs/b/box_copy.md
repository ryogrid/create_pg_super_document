# box_copy

## Location
[src/backend/access/spgist/spgproc.c:82-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgproc.c#L82-L88)

## Overview
Creates a deep copy of a BOX structure by allocating new memory and copying the original box's contents.

## Definition

```c
BOX *
box_copy(BOX *orig)
```
## Detailed Description
This utility function performs a deep copy of a BOX structure, which represents an axis-aligned bounding box in 2D space. The function allocates new memory using palloc() to store the copied box, ensuring that modifications to the copy do not affect the original box structure. This is essential for SP-GiST operations where box structures need to be modified or preserved independently during tree traversal and node splitting operations.

The copy operation transfers all coordinate data from the original box (including low and high corner points) to the newly allocated memory space.

## Parameters / Member Variables
- `*orig`: Pointer to the original BOX structure to be copied
## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (geometric box data structure)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
- Called from (representative examples):
  - [spg_kd_inner_consistent](../s/spg_kd_inner_consistent.md)
  - [spg_quad_inner_consistent](../s/spg_quad_inner_consistent.md)

## Notes and Other Information
- The returned pointer points to newly allocated memory that must be freed by the caller
- Used in SP-GiST index operations where independent copies of bounding boxes are needed
- Simple but essential utility for memory management in spatial indexing operations
- Performs a shallow copy of the BOX contents using structure assignment (*result = *orig)
- Memory allocation uses PostgreSQL's palloc which integrates with the database's memory context system