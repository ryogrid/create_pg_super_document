# SeenRelsEntry

## Location
src/backend/catalog/pg_inherits.c: 37 - 41

## Overview
A hash table entry structure used internally by the `find_all_inheritors` function to efficiently track visited relations during inheritance tree traversal and maintain their position in output lists.

## Definition
```c
typedef struct SeenRelsEntry
{
    Oid         rel_id;         /* relation oid */
    int         list_index;     /* its position in output list(s) */
} SeenRelsEntry;
```

## Detailed Description
`SeenRelsEntry` is a specialized data structure that serves as a hash table entry for tracking relations that have already been processed during inheritance hierarchy traversal in PostgreSQL. It is used exclusively within the `find_all_inheritors` function to implement an efficient O(1) lookup mechanism that prevents duplicate processing of relations in complex inheritance graphs.

The structure enables the function to:
- Detect when a relation has already been encountered (avoiding cycles and duplicates)
- Maintain the position of each relation in the output lists
- Support efficient lookups during the breadth-first traversal of the inheritance tree

This approach is particularly important when dealing with multiple inheritance paths, where a child relation might be reachable through different parent relations, ensuring that each relation appears only once in the final result.

## Parameters / Member Variables
- `rel_id`: The Object Identifier (OID) of the relation being tracked. This serves as the hash key for lookups in the hash table.
- `list_index`: The zero-based position of this relation in the output lists (`rels_list` and optionally `rel_numparents`). This allows the function to efficiently locate and update information about previously processed relations.

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - find_all_inheritors (used as hash table entry type)

## Notes and Other Information
- This structure is used exclusively as a hash table entry type with PostgreSQL's hash table implementation (HTAB)
- The hash table using this entry type is created temporarily within `find_all_inheritors` and destroyed when the function completes
- The structure is optimized for performance during inheritance tree traversal, supporting the function's ability to handle complex inheritance hierarchies efficiently
- Located in src/backend/catalog/pg_inherits.c:37-41
- The comment above the structure definition explicitly states it is "Entry of a hash table used in find_all_inheritors"