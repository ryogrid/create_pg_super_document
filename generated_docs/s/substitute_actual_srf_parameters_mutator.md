# substitute_actual_srf_parameters_mutator

## Location
[src/backend/optimizer/util/clauses.c:5373-5417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L5373-L5417)

## Overview
The actual mutator function that performs the substitution of Param nodes with real argument expressions during set-returning function inlining, handling proper variable level adjustments.

## Definition


## Detailed Description
This function implements the core logic for parameter substitution in set-returning function inlining. It serves as a tree mutator callback that is invoked for each node in the query tree during traversal. The function specifically handles Param nodes by replacing them with the corresponding actual argument expressions, while properly adjusting variable sublevel references to maintain correct scoping semantics in the modified query tree.

The function performs several key operations: it recursively processes Query nodes while tracking sublevel depth, identifies and processes PARAM_EXTERN nodes that need substitution, validates parameter IDs to ensure they are within the expected range, and adjusts variable sublevels in the substituted expressions to account for the new context. The sublevel adjustment is crucial because when a parameter is substituted with an actual argument expression, any variables in that expression need to have their sublevel references updated to reflect their new position in the query tree hierarchy.

The function uses a combination of  for Query nodes and  for other expression nodes, ensuring comprehensive traversal of all parts of the query tree while maintaining the proper context for variable level adjustments.

## Parameters / Member Variables
- : The current node being processed during tree traversal (can be any Node type)
- : Context structure containing substitution parameters including:
  - : Number of function parameters
  - : List of actual argument expressions  
  - : Current sublevel depth for variable reference adjustment

## Dependencies
- Functions called/Symbols referenced:
  -  - handles traversal and mutation of Query nodes
  -  - handles traversal and mutation of expression nodes
  -  - creates deep copies of argument expressions for substitution
  -  - retrieves the nth argument from the arguments list
  -  - adjusts variable sublevel references in substituted expressions
  -  - parameter kind constant for external parameters
  -  - context structure type

- Called from (representative examples):
  -  - entry point function that sets up the mutation context
  - Recursively calls itself during tree traversal for nested nodes

## Notes and Other Information
- This is a static function local to , used exclusively as a callback for tree mutation
- Handles only PARAM_EXTERN parameters, ignoring other parameter types
- Performs bounds checking on parameter IDs to prevent invalid substitutions
- The sublevel adjustment mechanism () is critical for maintaining correct variable scoping after substitution
- Increments and decrements  when entering and exiting Query nodes to track nesting depth
- Creates deep copies of argument expressions to avoid sharing issues in the modified query tree
- Returns NULL for NULL input nodes, maintaining tree structure integrity
- Uses both query tree and expression tree mutators appropriately based on node type
- The function is designed to work with the PostgreSQL tree mutation framework, following its conventions and patterns