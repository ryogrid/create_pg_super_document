# new_list

## Location
[src/backend/nodes/list.c:91-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L91-L154)

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
  - [lappend](../l/lappend.md)
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

## Simplified Source

```c
// Simplified version of new_list
static List *new_list(NodeTag type, int min_size) {
    List *newlist;
    int max_size;

    Assert(min_size > 0);

    // Calculate allocation size with optimization strategy
#ifndef DEBUG_LIST_MEMORY_USAGE
    // Normal build: allocate extra space using power-of-2 sizing
    // Minimum 8 ListCell units for efficiency
    max_size = pg_nextpower2_32(Max(8, min_size + LIST_HEADER_OVERHEAD));
    max_size -= LIST_HEADER_OVERHEAD;
#else
    // Debug build: allocate exact size to test enlarge_list() code paths
    max_size = min_size;
#endif

    // Allocate List header and initial elements in single allocation
    newlist = (List *) palloc(offsetof(List, initial_elements) +
                              max_size * sizeof(ListCell));

    // Initialize list structure
    newlist->type = type;
    newlist->length = min_size;
    newlist->max_length = max_size;
    newlist->elements = newlist->initial_elements;

    return newlist;
}
```

Key simplifications made:
- Consolidated allocation strategy explanation into clear comments
- Removed detailed memory optimization comments while preserving core logic
- Maintained dual allocation strategy for normal vs debug builds
- Preserved essential memory layout optimizations
- Focused on the core functionality: efficient list allocation with growth headroom