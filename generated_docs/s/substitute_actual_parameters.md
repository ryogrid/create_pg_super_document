# substitute_actual_parameters

## Location
[src/backend/optimizer/util/clauses.c:4907-4919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4907-L4919)

## Overview
Replaces Param nodes in an expression tree with their corresponding actual parameter values, serving as a wrapper function for the parameter substitution process.

## Definition

```c
static Node *
substitute_actual_parameters(Node *expr, int nargs, List *args,
							 int *usecounts)
```
## Detailed Description
This function serves as a convenience wrapper for the parameter substitution process during function inlining. It sets up the necessary context structure containing the parameter information and delegates the actual tree traversal and substitution work to substitute_actual_parameters_mutator. The function is part of the SQL function inlining mechanism, where parameter references (, , etc.) in the function body need to be replaced with the actual argument expressions provided in the function call.

The usecounts array is updated during the substitution process to track how many times each parameter is referenced, which is used by the calling code to determine whether inlining is safe (parameters used multiple times must not be expensive or volatile).

## Parameters / Member Variables
- : The expression tree containing Param nodes to be substituted
- : Number of function arguments/parameters
- : List of actual argument expressions to substitute
- : Array to track usage count of each parameter (output parameter)

## Dependencies
- Functions called/Symbols referenced:
  - [substitute_actual_parameters_context](substitute_actual_parameters_context.md) (context structure for parameter substitution)
  - [substitute_actual_parameters_mutator](substitute_actual_parameters_mutator.md) (performs the actual tree traversal and substitution)
- Called from:
  - [inline_function](../i/inline_function.md) (during SQL function inlining process)

## Notes and Other Information
- This is a static function used internally within clauses.c
- Acts as a simple wrapper that sets up context for the actual mutator function
- The usecounts array is modified during substitution to track parameter usage patterns
- Part of the larger function inlining optimization infrastructure
- Located in src/backend/optimizer/util/clauses.c at lines 4907-4919

## Simplified Source

```c
static Node *
substitute_actual_parameters(Node *expr, int nargs, List *args, int *usecounts)
{
    // Set up context for parameter substitution
    substitute_actual_parameters_context context;
    context.nargs = nargs;
    context.args = args;
    context.usecounts = usecounts;

    // Delegate to mutator for actual tree traversal
    return substitute_actual_parameters_mutator(expr, &context);
}
```