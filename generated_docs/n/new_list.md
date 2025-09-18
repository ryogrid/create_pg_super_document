# new_list

## Location
src/backend/nodes/list.c: 91 - 154

## Overview
A static function that allocates and initializes a new List structure with room for at least the specified minimum number of cells, implementing efficient memory allocation strategies.

## Definition
```c
static List *new_list(NodeTag type, int min_size)
```

## Detailed Description
This function creates a fresh List with optimized memory allocation. It allocates the List header and initial element storage in a single palloc() call for efficiency. The function implements two allocation strategies: in normal builds, it allocates extra cells beyond the minimum requirement using power-of-2 sizing to allow growth without reallocation; in debug builds with DEBUG_LIST_MEMORY_USAGE defined, it allocates exactly the minimum size to force testing of the enlarge_list() path. The function sets the initial length to min_size, marking those cells as valid, with the caller responsible for populating them.

## Parameters / Member Variables
- `type`: The NodeTag specifying the type of list (T_List, T_IntList, T_OidList, or T_XidList)
- `min_size`: The minimum number of cells that must be available. Must be greater than 0 since empty non-NIL lists are invalid.

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md) (calculates next power of 2)
  - LIST_HEADER_OVERHEAD (macro for header size calculation)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocator)
  - offsetof (standard C macro)
  - Max (macro for maximum value)
  - DEBUG_LIST_MEMORY_USAGE (conditional compilation flag)

- Called from (representative examples):
  - [list_make1_impl](../l/list_make1_impl.md)
  - [list_make2_impl](../l/list_make2_impl.md)  
  - lappend
  - [lcons](../l/lcons.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - [list_copy](../l/list_copy.md)
  - [pg_parse_query](../p/pg_parse_query.md)
  - [pg_rewrite_query](../p/pg_rewrite_query.md)

## Notes and Other Information
- Static function internal to list.c, not part of the public API
- Uses sophisticated memory allocation strategy: allocates List header and elements in single palloc() call for cache efficiency
- Normal builds use power-of-2 allocation sizes (minimum 8 ListCell units) to optimize for future growth
- Debug builds can force exact allocation to test memory management paths
- The allocated list has its length set to min_size, making those cells immediately valid for use
- Designed to minimize memory fragmentation and allocation overhead for typical short lists
- Contains extensive comments explaining allocation strategy trade-offs