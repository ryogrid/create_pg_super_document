# simplify_and_arguments

## Location
[src/backend/optimizer/util/clauses.c:3896-3989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L3896-L3989)

## Overview
A specialized function that processes and optimizes the arguments of AND boolean expressions during constant expression evaluation, handling nested AND flattening and constant folding with AND-specific logic.

## Definition


## Detailed Description
This function serves as a subroutine for  to optimize AND clause arguments, implementing the logical dual of . It performs several key operations:

1. **Nested AND Flattening**: Converts nested AND expressions like  into flat lists  for simplified reasoning
2. **Recursive Simplification**: Applies constant expression evaluation to each argument
3. **Constant Handling**: Processes constant values according to AND logic:
   - TRUE constants are dropped (don't affect AND result)
   - FALSE constants force the entire AND to FALSE
   - NULL constants are tracked but only one is kept
4. **Stack Overflow Prevention**: Uses an iterative approach with an unprocessed arguments list instead of deep recursion

The function mirrors the structure of  but implements the inverse logic for AND operations, where FALSE acts as the absorbing element (like TRUE for OR) and TRUE acts as the identity element (like FALSE for OR).

## Parameters / Member Variables
- : Input list of AND clause arguments to be processed and simplified
- : Evaluation context passed to recursive constant expression evaluation calls
- : Output parameter set to true if any NULL constant is found (must be initialized to false by caller)
- : Output parameter set to true if any FALSE constant is found (must be initialized to false by caller)

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a copy of the input arguments list
  -  - Removes first element from unprocessed arguments
  -  - Checks if a node is an AND boolean expression
  -  - Boolean expression node type for accessing AND arguments
  -  - Concatenates lists for flattening nested ANDs
  -  - Memory cleanup for old argument lists
  -  - Recursively simplifies individual arguments
  -  - Adds simplified arguments to result list
- Called from (representative examples):
  -  - Main constant expression evaluation function
  -  - Used in parallel query hazard assessment

## Notes and Other Information
- This is a static function, limiting its scope to the clauses.c file
- Returns NIL immediately when a FALSE constant is detected, optimizing for early termination
- Implements iterative processing to prevent stack overflow from deep AND nesting
- Handles the edge case where simplification might convert a non-AND into an AND
- Critical for maintaining SQL AND semantics: returns FALSE if any argument is FALSE, NULL if no argument is FALSE but at least one is NULL, TRUE otherwise
- The caller is responsible for adding the actual NULL constant to the final result when haveNull is true
- Uses the same memory management approach as  to avoid list leakage