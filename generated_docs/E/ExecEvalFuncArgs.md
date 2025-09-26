# ExecEvalFuncArgs

## Location
[src/backend/executor/execSRF.c:834-863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execSRF.c#L834-L863)

## Overview
ExecEvalFuncArgs evaluates the argument expressions for a function call and populates the FunctionCallInfo structure with the resulting values and null flags.

## Definition
```c
static void ExecEvalFuncArgs(FunctionCallInfo fcinfo, List *argList, ExprContext *econtext)
```

## Detailed Description
This function is a utility for evaluating function arguments within PostgreSQL's executor framework. It takes a list of expression states representing function arguments, evaluates each expression in the given expression context, and stores the results in the provided FunctionCallInfo structure. The function handles both the actual values and their corresponding null indicators, preparing the complete argument information needed for function invocation.

The function iterates through the argument list in order, evaluating each expression and storing both the computed value and its null status in the appropriate slots of the fcinfo->args array. This preparation step is essential before calling any PostgreSQL function through the function manager interface.

## Parameters / Member Variables
- `fcinfo`: Pointer to FunctionCallInfo structure where evaluated argument values and null flags will be stored
- `argList`: List of ExprState nodes representing the function arguments to be evaluated
- `econtext`: Expression context providing the evaluation environment and variable bindings

## Dependencies
- Functions called/Symbols referenced:
  - [ExecEvalExpr](ExecEvalExpr.md) (to evaluate individual argument expressions)
  - lfirst (to access list elements)
  - Assert (for debugging assertions)
- Called from (representative examples):
  - [ExecMakeTableFunctionResult](ExecMakeTableFunctionResult.md) (for table function argument evaluation)
  - [ExecMakeFunctionResultSet](ExecMakeFunctionResultSet.md) (for set-returning function argument evaluation)

## Notes and Other Information
- This is a static function, only accessible within the execSRF.c compilation unit
- The function includes an assertion to verify that the number of evaluated arguments matches the expected count in fcinfo->nargs
- Each argument evaluation can potentially set both a value and a null indicator, supporting PostgreSQL's three-valued logic
- The function assumes that the fcinfo structure has been properly initialized with the correct nargs count before being called
- This function is part of the set-returning function (SRF) execution infrastructure but is used for general function argument preparation