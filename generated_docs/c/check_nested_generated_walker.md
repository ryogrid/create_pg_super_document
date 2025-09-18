# check_nested_generated_walker

## Location
src/backend/catalog/heap.c: 2746 - 2787

## Overview
A static walker function that recursively traverses expression nodes to detect and prevent references to generated columns within column generation expressions, ensuring that generated columns do not depend on other generated columns.

## Definition
static bool check_nested_generated_walker(Node *node, void *context)

## Detailed Description
This function implements a node tree walker that validates expressions used in generated column definitions. It specifically prevents two types of invalid references:
1. Direct references to other generated columns (which would create dependency chains)
2. Whole-row variable references (which would cause self-referential dependencies)

The function operates as part of PostgreSQL's expression validation system, using the standard tree walker pattern to recursively examine all nodes in an expression tree. When it encounters a Var node (column reference), it checks whether the referenced column is generated and raises appropriate errors if violations are found.

## Parameters / Member Variables
- `node`: The current Node being examined in the expression tree traversal
- `context`: A ParseState pointer containing parser state information including the range table

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch: Retrieves relation information from the range table
  - get_attgenerated: Checks if an attribute is a generated column
  - get_attname: Gets the name of an attribute for error reporting
  - expression_tree_walker: Recursively walks the expression tree
  - ereport: Reports errors with detailed messages

- Called from (representative examples):
  - check_nested_generated: Main entry point for generated column validation
  - check_nested_generated_walker: Recursive self-calls during tree traversal

## Notes and Other Information
- This is a static function used internally within heap.c for generated column validation
- The function follows PostgreSQL's standard tree walker pattern, returning false to continue traversal or true to stop
- Error messages provide specific details about why the reference is invalid, including column names and parser positions
- System columns are explicitly excluded from validation as they are handled separately in the parser
- The function is part of PostgreSQL's generated column feature introduced to maintain data consistency