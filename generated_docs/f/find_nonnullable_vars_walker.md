# find_nonnullable_vars_walker

## Location
src/backend/optimizer/util/clauses.c: 1713 - 1915

## Overview
A recursive walker function that analyzes expression nodes to determine which variables must be nonnullable for the expression to return TRUE or avoid NULL results.

## Definition
static List *find_nonnullable_vars_walker(Node *node, bool top_level)

## Detailed Description
This is the core implementation function that performs the actual traversal and analysis of expression trees to identify nonnullable variables. It handles different types of expression nodes with specialized logic for each:

- **Variables**: Level-zero Vars are added to the result set using multibitmapset operations
- **Lists**: Treats implicit-AND lists at top level and strict function arguments uniformly
- **Function/Operator expressions**: Analyzes strict functions and operators, recursively examining their arguments
- **Boolean expressions**: Handles AND, OR, and NOT with different semantics based on top_level flag
- **Type coercion nodes**: Passes through most type coercions while maintaining strictness
- **Special tests**: Handles NULL tests and Boolean tests that can prove nonnullability
- **Subplans**: Analyzes subquery expressions for nonnullable constraints
- **PlaceHolders**: Recursively examines placeholder variable expressions

The function uses different semantics based on the top_level parameter: at top level, it seeks variables that cause FALSE-or-NULL results, while below top level it seeks variables that cause NULL results in strict contexts.

## Parameters / Member Variables
- node: The expression node to analyze for nonnullable variable constraints
- top_level: Boolean flag indicating whether analyzing top-level Boolean context (TRUE) or strict function context (FALSE)

## Dependencies
- Functions called/Symbols referenced:
  - mbms_add_member
  - mbms_add_members
  - mbms_int_members
  - [func_strict](func_strict.md)
  - set_opfuncid
  - [is_strict_saop](../i/is_strict_saop.md)
  - [find_nonnullable_vars_walker](find_nonnullable_vars_walker.md) (recursive calls)
- Called from (representative examples):
  - [find_nonnullable_vars](find_nonnullable_vars.md)
  - [find_nonnullable_vars_walker](find_nonnullable_vars_walker.md) (recursive)

## Notes and Other Information
- This is a static function internal to clauses.c
- Uses multibitmapset operations for efficiently managing variable sets across relations
- Handles complex Boolean logic with intersection semantics for OR expressions
- Special handling for array coercion expressions that are strict at array level but not element level
- Supports PlaceHolderVar nodes for handling placeholder variables in query planning
- The recursive nature allows deep analysis of nested expressions while maintaining proper strictness contexts