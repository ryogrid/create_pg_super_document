# pull_vars_context

## Location
[src/backend/optimizer/util/var.c:49-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L49-L54)

## Overview
A context structure used by the pull_vars_of_level functionality to collect Var and PlaceHolderVar nodes from a specific query level during expression tree traversal.

## Definition
```c
typedef struct
{
    List       *vars;
    int         sublevels_up;
} pull_vars_context;
```

## Detailed Description
The pull_vars_context structure serves as a walker context for the pull_vars_walker function, which traverses expression trees to collect all Var and PlaceHolderVar nodes that belong to a specific query nesting level. This functionality is essential for query optimization tasks that need to analyze variable references at particular subquery levels, such as correlated subquery optimization and variable dependency analysis. The context maintains a list of collected variables and tracks the target query level for filtering.

## Parameters / Member Variables
- `vars`: A List containing pointers to collected Var and PlaceHolderVar nodes from the target query level
- `sublevels_up`: Integer specifying the target subquery nesting level to collect variables from (0 for current level)

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (typedef)
  - [Var](../V/Var.md) (struct)
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (struct)
- Called from (representative examples):
  - [pull_vars_of_level](pull_vars_of_level.md)
  - [pull_vars_walker](pull_vars_walker.md)
  - flatten_join_alias_vars_context

## Notes and Other Information
This context structure is designed to collect actual Var node pointers rather than just their identifiers, enabling detailed analysis of variable references. The walker processes both Var nodes (matching varlevelsup) and PlaceHolderVar nodes (matching phlevelsup) from the specified query level. The collected variables are not copied but linked directly into the list, so callers must be careful about the lifetime of the returned nodes. This functionality is crucial for optimizations involving correlated subqueries and variable dependency tracking.