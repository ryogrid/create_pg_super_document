# max_parallel_hazard_checker

## Location
src/backend/optimizer/util/clauses.c: 822 - 828

## Overview
A callback function that checks the parallel safety of individual functions during expression tree traversal by retrieving their parallel classification and delegating to the core hazard testing logic.

## Definition


## Detailed Description
The  function serves as a callback interface for the  infrastructure. When the expression tree walker encounters function calls, it uses this callback to determine the parallel safety of each function.

The function operates by:
1. Taking a function's OID (object identifier) and looking up its parallel classification using 
2. Passing this classification to  along with the traversal context
3. Returning the result, which indicates whether traversal should continue or stop

This design separates the concern of function lookup (handled here) from the hazard level evaluation logic (handled in ). The callback pattern allows the generic tree walking infrastructure to remain agnostic about the specific parallel safety logic while providing a clean interface for function-specific checks.

## Parameters / Member Variables
- : Object identifier (OID) of the function to check for parallel safety
- : Void pointer to max_parallel_hazard_context structure, cast appropriately within the function

## Dependencies
- Functions called/Symbols referenced:
  - max_parallel_hazard_test
  - func_parallel
  - max_parallel_hazard_context
- Called from (representative examples):
  - max_parallel_hazard_walker

## Notes and Other Information
- This is a static function designed specifically as a callback for the check_functions_in_node infrastructure
- The void pointer parameter follows the standard callback pattern in C, requiring a cast to the appropriate context type
- The function acts as a thin wrapper that bridges the generic function-checking infrastructure with the specific parallel hazard evaluation logic
- Uses the func_parallel() system function to retrieve the parallel classification from PostgreSQL's system catalogs
- Located in src/backend/optimizer/util/clauses.c:822-828