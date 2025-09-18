# locate_windowfunc_context

## Location
[src/backend/rewrite/rewriteManip.c:42-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L42-L48)

## Overview
A context structure used to capture the parse location when searching for window functions in an expression tree.

## Definition
```c
typedef struct
{
    int         win_location;
} locate_windowfunc_context;
```

## Detailed Description
This structure serves as a context parameter for the tree walker functions that locate window functions and capture their parse location. Unlike the aggregate-related contexts, this structure is simpler because window functions only operate at the current query level (they don't have the concept of levels up like aggregates do).

The context is used in conjunction with `locate_windowfunc()` function to find the parse location of window functions in a query tree. This is particularly important for generating accurate error messages that can point users to the exact location in their SQL query where problematic window functions occur.

## Parameters / Member Variables
- `win_location`: Stores the parse location (character offset) of the first window function found; initialized to -1 if no window function is found or if all found window functions have unknown parse locations

## Dependencies
- Functions called/Symbols referenced: None (pure data structure)
- Called from (representative examples):
  - [locate_windowfunc](locate_windowfunc.md) (src/backend/rewrite/rewriteManip.c:256)
  - [locate_windowfunc_walker](locate_windowfunc_walker.md) (src/backend/rewrite/rewriteManip.c:273)

## Notes and Other Information
- Part of PostgreSQL's query rewriting infrastructure in rewriteManip.c
- Primarily used for error reporting purposes rather than performance-critical operations
- Unlike aggregate functions, window functions don't have sublevel tracking since they operate only at the current query level
- The walker function stops traversal immediately upon finding the first window function with a valid location
- Does not recurse into subselects, as window functions are limited to the current query level
- Returns -1 when no window functions are found or when all found window functions have unknown parse locations