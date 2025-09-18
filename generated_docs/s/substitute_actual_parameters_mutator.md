# substitute_actual_parameters_mutator

## Location
src/backend/optimizer/util/clauses.c: 4920 - 4948

## Overview
Performs the actual tree traversal and node substitution to replace Param nodes with their corresponding actual argument expressions during function inlining.

## Definition


## Detailed Description
This function implements the core logic for parameter substitution during SQL function inlining. It uses the expression_tree_mutator framework to traverse the expression tree and replace each Param node (representing function parameters like $1, $2, etc.) with the corresponding actual argument expression from the function call.

When a Param node is encountered, the function validates that it's an external parameter (PARAM_EXTERN) with a valid parameter ID within the expected range. It increments the usage counter for that parameter and returns the corresponding argument from the context's args list. For all other node types, it recursively processes child nodes using expression_tree_mutator.

The function is designed to be called recursively through the expression_tree_mutator mechanism, ensuring that all Param nodes throughout the expression tree are properly replaced.

## Parameters / Member Variables
- : The current node being processed in the expression tree traversal
- : Context structure containing parameter substitution information including nargs (number of arguments), args (list of actual arguments), and usecounts (array to track parameter usage)

## Dependencies
- Functions called/Symbols referenced:
  - substitute_actual_parameters_context (context structure type)
  - Param (parameter node type)
  - PARAM_EXTERN (external parameter constant)
  - list_nth (gets nth element from list)
  - expression_tree_mutator (generic tree traversal function)
- Called from:
  - substitute_actual_parameters (initial entry point)
  - substitute_actual_parameters_mutator (recursive calls during tree traversal)

## Notes and Other Information
- This is a static function used internally within clauses.c
- Uses the standard PostgreSQL expression_tree_mutator pattern for tree traversal
- Validates parameter kinds and IDs with error reporting for invalid cases
- Updates usage counters which are used by calling code to determine inlining safety
- Parameter IDs are 1-based (as seen in SQL with $1, $2, etc.) but arrays are 0-based
- Does not copy nodes during substitution - copying is deferred to later stages
- Located in src/backend/optimizer/util/clauses.c at lines 4920-4948