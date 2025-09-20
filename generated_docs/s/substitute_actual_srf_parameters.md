# substitute_actual_srf_parameters

## Location
[src/backend/optimizer/util/clauses.c:5358-5372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L5358-L5372)

## Overview
Replaces Param nodes in a Query tree with appropriate actual parameters during set-returning function inlining, using a specialized tree mutation approach.

## Definition

```c
static Query *
substitute_actual_srf_parameters(Query *expr, int nargs, List *args)
```
## Detailed Description
This function serves as the entry point for parameter substitution specifically designed for set-returning function inlining. It creates a context structure containing the parameter information and delegates the actual substitution work to a query tree mutator function. The function is similar to  but has specific behavior tailored for set-returning function contexts, particularly in how it handles parameter scoping levels.

The function sets up a substitution context that tracks the number of parameters, the list of actual argument expressions, and the current sublevel (initialized to 1 to account for the function's parameter scope). It then applies the  framework to traverse the entire query tree and perform parameter substitutions wherever Param nodes are encountered.

The key difference from regular parameter substitution is in the handling of sublevels, which is crucial for properly resolving parameter references in the context of inlined set-returning functions where the parameter scope relationship differs from regular function inlining.

## Parameters / Member Variables
- : The Query tree in which to perform parameter substitution
- : The number of function parameters to substitute
- : List of actual argument expressions to substitute for the parameters

## Dependencies
- Functions called/Symbols referenced:
  -  - context structure for parameter substitution
  -  - generic query tree traversal and mutation framework
  -  - the actual mutation function that performs substitutions

- Called from (representative examples):
  -  - during set-returning function inlining to substitute parameters with actual arguments

## Notes and Other Information
- This is a static function local to , used exclusively for set-returning function parameter substitution
- The function is designed specifically for set-returning function contexts and differs from regular parameter substitution
- Sets  to 1 initially, which is appropriate for the parameter scoping in set-returning function inlining
- Uses the query tree mutator framework to ensure all parts of the query tree are properly traversed and modified
- The actual substitution logic is implemented in the companion mutator function 
- Returns a modified copy of the input query tree with all applicable Param nodes replaced by actual argument expressions