# substitute_actual_parameters_mutator

## Location
[src/backend/optimizer/util/clauses.c:4920-4948](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4920-L4948)

## Overview
Performs the actual tree traversal and node substitution to replace Param nodes with their corresponding actual argument expressions during function inlining.

## Definition

```c
static Node *
substitute_actual_parameters_mutator(Node *node,
									 substitute_actual_parameters_context *context)
```
## Detailed Description
This function implements the core logic for parameter substitution during SQL function inlining. It uses the expression_tree_mutator framework to traverse the expression tree and replace each Param node (representing function parameters like $1, $2, etc.) with the corresponding actual argument expression from the function call.

When a Param node is encountered, the function validates that it's an external parameter (PARAM_EXTERN) with a valid parameter ID within the expected range. It increments the usage counter for that parameter and returns the corresponding argument from the context's args list. For all other node types, it recursively processes child nodes using expression_tree_mutator.

The function is designed to be called recursively through the expression_tree_mutator mechanism, ensuring that all Param nodes throughout the expression tree are properly replaced.

## Parameters / Member Variables
- : The current node being processed in the expression tree traversal
- : Context structure containing parameter substitution information including nargs (number of arguments), args (list of actual arguments), and usecounts (array to track parameter usage)

## Dependencies
- Functions called/Symbols referenced:
  - [substitute_actual_parameters_context](substitute_actual_parameters_context.md) (context structure type)
  - [Param](../P/Param.md) (parameter node type)
  - PARAM_EXTERN (external parameter constant)
  - [list_nth](../l/list_nth.md) (gets nth element from list)
  - expression_tree_mutator (generic tree traversal function)
- Called from:
  - [substitute_actual_parameters](substitute_actual_parameters.md) (initial entry point)
  - [substitute_actual_parameters_mutator](substitute_actual_parameters_mutator.md) (recursive calls during tree traversal)

## Notes and Other Information
- This is a static function used internally within clauses.c
- Uses the standard PostgreSQL expression_tree_mutator pattern for tree traversal
- Validates parameter kinds and IDs with error reporting for invalid cases
- Updates usage counters which are used by calling code to determine inlining safety
- Parameter IDs are 1-based (as seen in SQL with $1, $2, etc.) but arrays are 0-based
- Does not copy nodes during substitution - copying is deferred to later stages
- Located in src/backend/optimizer/util/clauses.c at lines 4920-4948

## Simplified Source

```c
static Node *substitute_actual_parameters_mutator(Node *node,
                                                 substitute_actual_parameters_context *context) {
    if (node == NULL)
        return NULL;

    // Handle parameter nodes - replace with actual arguments
    if (IsA(node, Param)) {
        Param *param = (Param *) node;

        // Validate parameter type and ID range
        if (param->paramkind != PARAM_EXTERN)
            elog(ERROR, "unexpected paramkind: %d", (int) param->paramkind);
        if (param->paramid <= 0 || param->paramid > context->nargs)
            elog(ERROR, "invalid paramid: %d", param->paramid);

        // Track parameter usage and return corresponding argument
        context->usecounts[param->paramid - 1]++;
        return list_nth(context->args, param->paramid - 1);
    }

    // Recursively process other node types
    return expression_tree_mutator(node, substitute_actual_parameters_mutator, context);
}
```