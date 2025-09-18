# find_expr_references_walker

## Location
src/backend/catalog/dependency.c: 1698 - 2320

## Overview
Recursively traverses an expression tree to identify and collect all database object references, serving as the core dependency discovery engine for PostgreSQL's dependency tracking system.

## Definition


## Detailed Description
This function is the heart of PostgreSQL's expression dependency analysis system. It implements a comprehensive tree walker that recursively examines every node in an expression tree to identify references to database objects such as tables, columns, operators, functions, types, collations, and more.

The function handles dozens of different node types, each with specific logic for extracting object references. It employs intelligent dependency tracking to avoid redundant dependencies - for instance, it doesn't create type dependencies when there's already an indirect dependency through an operator or function.

Key responsibilities include:
- Variable (Var) resolution through range tables 
- Constant value analysis for reg* type literals
- Function and operator reference extraction
- Type and collation dependency tracking
- Subquery and CTE processing
- Complex expression type handling (FieldSelect, ArrayCoerceExpr, etc.)
- Special handling for INSERT/UPDATE target columns

The walker uses the standard PostgreSQL expression_tree_walker framework and maintains context about the current range table stack for proper variable resolution across nested queries.

## Parameters / Member Variables
- : Current node in the expression tree being examined
- : Context structure containing collected object addresses and range table stack

## Dependencies
- Functions called/Symbols referenced:
  - [add_object_address](../a/add_object_address.md) (primary dependency recording function)
  - expression_tree_walker (recursive tree traversal framework)
  - query_tree_walker (for Query node traversal)
  - [process_function_rte_ref](../p/process_function_rte_ref.md) (for function RTE column analysis)
  - [getBaseType](../g/getBaseType.md), get_typ_typrelid (type analysis utilities)
  - SearchSysCacheExists1 (catalog existence checks)
  - Various list manipulation functions (list_nth, lcons, etc.)
- Called from (representative examples):
  - [recordDependencyOnExpr](../r/recordDependencyOnExpr.md) (main entry point)
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md) (single-relation variant)
  - Self-recursion for complex nested structures

## Notes and Other Information
- Handles over 25 different expression node types with specialized logic for each
- Implements optimized dependency tracking to avoid redundant type/collation dependencies
- Uses range table context stack to properly resolve variables across query nesting levels
- Special handling for reg* literal types that reference specific database objects
- Automatically processes subqueries and CTEs recursively
- Critical for maintaining referential integrity in PostgreSQL's catalog system
- The function deliberately avoids creating dependencies for whole-row references, leaving that responsibility to the range table level
- Contains extensive error handling for malformed expressions and invalid references