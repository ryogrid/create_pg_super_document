# locate_var_of_level_context

## Location
src/backend/optimizer/util/var.c: 55 - 60

## Overview
A context structure used by the locate_var_of_level functionality to find the parse location of Var nodes from a specific query level during expression tree traversal, primarily for error reporting purposes.

## Definition
```c
typedef struct
{
    int         var_location;
    int         sublevels_up;
} locate_var_of_level_context;
```

## Detailed Description
The locate_var_of_level_context structure serves as a walker context for the locate_var_of_level_walker function, which traverses expression trees to find the first Var node from a specified query level that has a valid parse location. This functionality is primarily used for error reporting, where PostgreSQL needs to provide accurate source code location information when reporting errors related to variables at specific subquery levels. The walker stops as soon as it finds the first qualifying Var with a valid location.

## Parameters / Member Variables
- `var_location`: Integer storing the parse location of the found Var node, initialized to -1 if no valid location is found
- `sublevels_up`: Integer specifying the target subquery nesting level to search for variables (0 for current level)

## Dependencies
- Functions called/Symbols referenced:
  - Var (struct)
  - Parse location system
- Called from (representative examples):
  - [locate_var_of_level](locate_var_of_level.md)
  - [locate_var_of_level_walker](locate_var_of_level_walker.md)
  - flatten_join_alias_vars_context

## Notes and Other Information
This context structure is specifically designed for error reporting and diagnostic purposes rather than optimization. The walker terminates early (returns true) as soon as it finds the first Var node with a valid location at the target query level, making it efficient for its intended use case. The function only considers Var nodes with location >= 0, as negative values indicate unknown parse locations. This functionality helps PostgreSQL provide meaningful error messages that can point users to the exact source code location where problematic variables are referenced.