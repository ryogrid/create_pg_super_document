# clean_NOT

## Location
[src/backend/utils/adt/tsquery_cleanup.c:190-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_cleanup.c#L190-L237)

## Overview
Cleans and optimizes a TSQuery expression tree by removing redundant NOT operators, which always return TRUE and can make the query inefficient.

## Definition

```c
QueryItem *
clean_NOT(QueryItem *ptr, int *len)
```
## Detailed Description
The  function processes a TSQuery (text search query) represented as a flat array of QueryItem structures and removes unnecessary NOT operators from the query tree. NOT operators in PostgreSQL's text search always return TRUE, so they can be safely removed to optimize query performance. The function converts the flat QueryItem array into a tree structure, applies the cleanup logic recursively, and then converts it back to a flat array format.

The cleanup process handles different operator types:
- **NOT operators**: Completely removed from the tree as they're redundant
- **OR operators**: If either operand becomes NULL after cleanup, the entire OR subtree is removed
- **AND/PHRASE operators**: Simplified by removing NULL operands, promoting single remaining operands

## Parameters / Member Variables
- `*ptr`: Input array of QueryItem structures representing the TSQuery in postfix notation
- `*len`: Pointer to integer containing the length of the input array; updated with the length of the cleaned array
## Dependencies
- Functions called/Symbols referenced:
  - : Converts flat QueryItem array to tree structure
  - : Recursively cleans NOT operators from the tree
  - : Converts cleaned tree back to flat QueryItem array
  - : Tree node structure type
  - : Query element structure type

- Called from (representative examples):
  - : Main TSQuery processing function in tsquery.c:1378

## Notes and Other Information
- This function is part of PostgreSQL's text search optimization pipeline
- The cleanup is primarily useful for debugging and index search optimization
- The function uses recursive tree traversal with stack depth checking to prevent overflow
- Memory management is handled carefully with  and  calls to avoid leaks
- The result may be a shorter QueryItem array if redundant operators were removed

## Simplified Source

```c
QueryItem *clean_NOT(QueryItem *ptr, int *len) {
    // Convert flat array to tree structure
    NODE *root = maketree(ptr);

    // Clean NOT operators from tree and convert back to flat array
    return plaintree(clean_NOT_intree(root), len);
}
```