# SimpleStringListCell

## Location
[src/include/fe_utils/simple_list.h:32-38](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/simple_list.h#L32-L38)

## Overview
SimpleStringListCell is a linked list node structure designed to store null-terminated strings with tracking functionality, providing an efficient way to manage collections of string data in PostgreSQL frontend utilities.

## Definition
```c
typedef struct SimpleStringListCell
{
    struct SimpleStringListCell *next;
    bool touched;          /* true, when this string was searched and
                            * touched */
    char val[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated string here */
} SimpleStringListCell;
```

## Detailed Description
SimpleStringListCell represents a single node in a singly-linked list specifically designed for string storage with additional tracking capabilities. The structure extends beyond basic linked list functionality by including a 'touched' flag that indicates whether the string has been accessed or processed during operations. The string data is stored using a flexible array member, allowing for variable-length strings while maintaining efficient memory usage. This design is particularly useful in PostgreSQL frontend tools where lists of database names, table names, or other string identifiers need to be managed and tracked during processing.

## Parameters / Member Variables
- `next`: Pointer to the next SimpleStringListCell in the linked list, or NULL if this is the last cell
- `touched`: Boolean flag indicating whether this string has been searched or accessed during processing operations
- `val`: Flexible array member containing the null-terminated string data; actual size is determined at allocation time

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleStringListCell](SimpleStringListCell.md) (self-reference for next pointer)
  - FLEXIBLE_ARRAY_MEMBER (PostgreSQL macro for variable-length arrays)
  - [bool](../b/bool.md) (boolean type)
- Called from (representative examples):
  - [simple_string_list_append](../s/simple_string_list_append.md)
  - [simple_string_list_member](../s/simple_string_list_member.md)
  - [simple_string_list_destroy](../s/simple_string_list_destroy.md)
  - [simple_string_list_not_touched](../s/simple_string_list_not_touched.md)
  - Various PostgreSQL frontend tools (pg_dump, clusterdb, reindexdb, vacuumdb)
  - [store_pub_sub_info](../s/store_pub_sub_info.md)
  - expand_*_name_patterns functions

## Notes and Other Information
- The 'touched' flag provides a mechanism for tracking which strings have been processed, useful for validation and cleanup operations
- Uses FLEXIBLE_ARRAY_MEMBER to store variable-length strings efficiently without additional memory allocation overhead
- Widely used across PostgreSQL frontend utilities for managing lists of database object names
- The structure is optimized for memory efficiency by placing the string data directly in the structure rather than using a separate allocation
- Common use cases include storing lists of database names, table names, schema names, and other string identifiers in command-line tools