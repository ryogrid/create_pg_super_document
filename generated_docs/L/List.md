# List

## Location
[src/include/nodes/pg_list.h:53-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L53-L62)

## Overview
A fundamental data structure in PostgreSQL that represents a dynamically resizable array of ListCell elements, supporting various data types including general pointers, integers, OIDs, and XIDs.

## Definition
```c
typedef struct List
{
    NodeTag     type;           /* T_List, T_IntList, T_OidList, or T_XidList */
    int         length;         /* number of elements currently present */
    int         max_length;     /* allocated length of elements[] */
    ListCell   *elements;       /* re-allocatable array of cells */
    /* We may allocate some cells along with the List header: */
    ListCell    initial_elements[FLEXIBLE_ARRAY_MEMBER];
    /* If elements == initial_elements, it's not a separate allocation */
} List;
```

## Detailed Description
The List structure is PostgreSQL's primary list implementation, providing a flexible and efficient way to store collections of data. It uses a NodeTag to distinguish between different types of lists (T_List for general pointers, T_IntList for integers, T_OidList for object identifiers, and T_XidList for transaction identifiers). The structure implements a dynamic array approach where elements are stored in a contiguous array of ListCell structures. For performance optimization, it includes an initial_elements array that can be allocated inline with the List header to avoid separate memory allocations for small lists.

## Parameters / Member Variables
- `type`: NodeTag indicating the specific list type (T_List, T_IntList, T_OidList, or T_XidList)
- `length`: Current number of elements stored in the list
- `max_length`: Total allocated capacity of the elements array
- `elements`: Pointer to the array of ListCell elements, which may point to initial_elements or separately allocated memory
- `initial_elements`: Inline array for small lists to optimize memory allocation

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - Various PostgreSQL subsystems that need dynamic list storage

## Notes and Other Information
The List structure is designed with memory efficiency in mind, using the flexible array member pattern to potentially store small lists without additional memory allocations. The elements pointer can either point to the inline initial_elements array or to a separately allocated array when the list grows beyond the initial capacity.