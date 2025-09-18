# locate_agg_of_level_context

## Location
[src/backend/rewrite/rewriteManip.c:37-41](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L37-L41)

## Overview
A context structure used to track both the target query level and the parse location when searching for aggregate functions in an expression tree.

## Definition
```c
typedef struct
{
    int         agg_location;
    int         sublevels_up;
} locate_agg_of_level_context;
```

## Detailed Description
This structure serves as a context parameter for the tree walker functions that locate aggregate functions at a specific query level and capture their parse location. Unlike the simpler `contain_aggs_of_level_context`, this structure also stores the parse location of any found aggregate, making it useful for error reporting and diagnostic purposes.

The context is used in conjunction with `locate_agg_of_level()` function to find the parse location of aggregates at a specified query level. This is particularly important for generating accurate error messages that can point users to the exact location in their SQL query where problematic aggregates occur.

## Parameters / Member Variables
- `agg_location`: Stores the parse location (character offset) of the first aggregate found at the target level; initialized to -1 if no aggregate is found or if all found aggregates have unknown parse locations
- `sublevels_up`: The target query level depth to search for aggregates; represents how many levels up from the current context to look for matching aggregates

## Dependencies
- Functions called/Symbols referenced: None (pure data structure)
- Called from (representative examples):
  - [locate_agg_of_level](locate_agg_of_level.md) (src/backend/rewrite/rewriteManip.c:152)
  - [locate_agg_of_level_walker](locate_agg_of_level_walker.md) (src/backend/rewrite/rewriteManip.c:171)

## Notes and Other Information
- Part of PostgreSQL's query rewriting infrastructure in rewriteManip.c
- Primarily used for error reporting purposes rather than performance-critical operations
- The walker function stops traversal immediately upon finding the first aggregate with a valid location
- Handles both Aggref and GroupingFunc node types
- Returns -1 when no aggregates are found or when all found aggregates have unknown parse locations