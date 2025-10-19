# evaluateExpr

## Location
[src/bin/pgbench/pgbench.c:2837-2879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2837-L2879)

## Overview
Performs recursive evaluation of an expression in a pgbench script using the current state of variables and returns the computed value.

## Definition

```c
static bool
evaluateExpr(CState *st, PgBenchExpr *expr, PgBenchValue *retval)
```
## Detailed Description
The  function is a core evaluation engine for pgbench expressions. It recursively processes different types of expression nodes (constants, variables, and functions) and computes their values based on the current execution state. The function uses a switch statement to handle different expression node types and returns a boolean indicating success or failure of the evaluation, with the actual computed value returned through the  pointer parameter.

The function handles three main expression types:
- **ENODE_CONSTANT**: Direct return of constant values
- **ENODE_VARIABLE**: Variable lookup and value retrieval with error handling for undefined variables
- **ENODE_FUNCTION**: Delegation to function evaluation via 

## Parameters / Member Variables
- `*st`: Pointer to the current client state containing variable bindings and execution context
- `*expr`: Pointer to the expression node to be evaluated
- `*retval`: Output parameter that receives the computed value upon successful evaluation
## Dependencies
- Functions called/Symbols referenced:
  - [lookupVariable](../l/lookupVariable.md)
  - [makeVariableValue](../m/makeVariableValue.md)  
  - [evalFunc](evalFunc.md)
  - pg_log_error
  - [pg_fatal](../p/pg_fatal.md)
- Types used:
  - [CState](../C/CState.md)
  - [PgBenchExpr](../P/PgBenchExpr.md)
  - PgBenchValue
  - [Variable](../V/Variable.md)
  - ENODE_CONSTANT
  - ENODE_VARIABLE
  - ENODE_FUNCTION
- Called from (representative examples):
  - [evalLazyFunc](evalLazyFunc.md)
  - [evalStandardFunc](evalStandardFunc.md)
  - [executeMetaCommand](executeMetaCommand.md)

## Notes and Other Information
- The function is declared as static, indicating it's for internal use within the pgbench module
- Error handling includes logging undefined variable errors and fatal errors for unexpected expression node types
- The recursive nature allows for complex nested expressions to be properly evaluated
- [Variable](../V/Variable.md) lookup includes validation to ensure variables exist before attempting to access their values

## Simplified Source

```c
static bool evaluateExpr(CState *st, PgBenchExpr *expr, PgBenchValue *retval) {
    switch (expr->etype) {
        case ENODE_CONSTANT:
            // Return constant value directly
            *retval = expr->u.constant;
            return true;

        case ENODE_VARIABLE: {
            // Look up variable by name
            Variable *var = lookupVariable(&st->variables, expr->u.variable.varname);
            if (var == NULL) {
                pg_log_error("undefined variable \"%s\"", expr->u.variable.varname);
                return false;
            }

            // Ensure variable has a valid value
            if (!makeVariableValue(var))
                return false;

            // Return variable's current value
            *retval = var->value;
            return true;
        }

        case ENODE_FUNCTION:
            // Delegate to function evaluation
            return evalFunc(st, expr->u.function.function, expr->u.function.args, retval);

        default:
            // Should never reach here
            pg_fatal("unexpected enode type in evaluation: %d", expr->etype);
    }
}
```