# check_parameter_resolution_walker

## Location
[src/backend/parser/parse_param.c:286-329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_param.c#L286-L329)

## Overview
A tree-walking function that validates parameter symbols match their assigned types throughout a fully-analyzed query tree, ensuring consistent parameter type resolution.

## Definition

```c
static bool
check_parameter_resolution_walker(Node *node, ParseState *pstate)
```
## Detailed Description
This static function serves as a tree walker that traverses a fully-analyzed query tree to verify that parameter symbols are consistently typed. It addresses the issue where some parameters might remain UNKNOWN if there was insufficient context to force their coercion, while other instances of the same parameter might have been coerced to specific types elsewhere in the query.

For each Param node of PARAM_EXTERN kind, the function validates that:
1. The parameter ID is within valid bounds (1 to numParams)
2. The parameter's type matches the type stored in the VarParamState

If inconsistencies are found, appropriate errors are reported with precise location information. The function recursively processes Query nodes and expression trees to ensure complete validation.

## Parameters / Member Variables
- `*node`: Current node being examined in the tree traversal
- `*pstate`: ParseState containing parser state and VarParamState information
## Dependencies
- Functions called/Symbols referenced:
  - [Param](../P/Param.md)
  - PARAM_EXTERN
  - [VarParamState](../V/VarParamState.md)
  - query_tree_walker
  - expression_tree_walker
  - [check_parameter_resolution_walker](check_parameter_resolution_walker.md) (recursive)
- Called from (representative examples):
  - [check_variable_parameters](check_variable_parameters.md)
  - [check_parameter_resolution_walker](check_parameter_resolution_walker.md) (recursive)

## Notes and Other Information
- This is a static function used internally within the parameter resolution system
- Located in src/backend/parser/parse_param.c:286-329
- Returns false to continue tree traversal (standard walker pattern)
- Generates specific error codes: ERRCODE_UNDEFINED_PARAMETER and ERRCODE_AMBIGUOUS_PARAMETER
- Handles both regular expression trees and Query substructures recursively
- Part of PostgreSQL's variable parameter type resolution and validation framework

## Simplified Source

```c
static bool
check_parameter_resolution_walker(Node *node, ParseState *pstate)
{
    if (node == NULL)
        return false;

    if (IsA(node, Param))
    {
        Param *param = (Param *) node;

        if (param->paramkind == PARAM_EXTERN)
        {
            VarParamState *parstate = (VarParamState *) pstate->p_ref_hook_state;
            int paramno = param->paramid;

            // Check parameter number is valid
            if (paramno <= 0 || paramno > *parstate->numParams)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_PARAMETER),
                     errmsg("there is no parameter $%d", paramno),
                     parser_errposition(pstate, param->location)));

            // Check parameter type matches expected type
            if (param->paramtype != (*parstate->paramTypes)[paramno - 1])
                ereport(ERROR,
                    (errcode(ERRCODE_AMBIGUOUS_PARAMETER),
                     errmsg("could not determine data type of parameter $%d", paramno),
                     parser_errposition(pstate, param->location)));
        }
        return false;
    }

    if (IsA(node, Query))
    {
        // Recurse into subqueries
        return query_tree_walker((Query *) node,
                                check_parameter_resolution_walker,
                                (void *) pstate, 0);
    }

    // Recurse into expression trees
    return expression_tree_walker(node, check_parameter_resolution_walker,
                                 (void *) pstate);
}
```