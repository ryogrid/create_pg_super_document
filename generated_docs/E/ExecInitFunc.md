# ExecInitFunc

## Location
src/backend/executor/execExpr.c: 2628 - 2732

## Overview
Performs setup necessary for the evaluation of function-like expressions by appending argument evaluation steps to the expression state and preparing function call structures for efficient runtime execution.

## Definition


## Detailed Description
ExecInitFunc is a critical internal function in PostgreSQL's expression evaluation system that initializes function call expressions during query plan setup. It handles all the preparatory work needed to execute function calls efficiently at runtime, including permission checks, argument setup, and opcode selection based on function characteristics.

The function performs several key operations:
1. **Security validation**: Checks ACL permissions to ensure the user can execute the specified function
2. **Function metadata setup**: Initializes FmgrInfo and FunctionCallInfo structures with function details
3. **Argument processing**: Handles both constant and variable arguments, optimizing constants by pre-evaluating them
4. **Opcode selection**: Chooses the appropriate execution opcode based on function strictness and statistics tracking requirements

The function is designed to be called during expression compilation and prepares a "scratch" ExprEvalStep that can be customized by callers before being pushed to the execution steps list.

## Parameters / Member Variables
- : Pre-allocated ExprEvalStep structure to be populated with function call setup data
- : The original Expr node representing the function call (used for error reporting and metadata)
- : List of argument expressions to be evaluated before function execution
- : OID of the function to be called, used for permission checks and function lookup
- : Collation ID to be used for the function call, affects string operations
- : Current ExprState containing the expression compilation context and step list

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclcheck](../o/object_aclcheck.md) (permission validation)
  - [fmgr_info](../f/fmgr_info.md) (function manager setup)
  - InitFunctionCallInfoData (function call structure initialization)
  - [ExecInitExprRec](ExecInitExprRec.md) (recursive argument expression setup)
  - [executor_errposition](../e/executor_errposition.md) (error position reporting)
- Called from (representative examples):
  - [ExecInitExprRec](ExecInitExprRec.md) (multiple call sites for different function expression types)

## Notes and Other Information
- This function does not push the prepared step to the execution list, allowing callers to modify the opcode for special cases like DISTINCT operations
- Includes safety checks for maximum argument count (FUNC_MAX_ARGS) and rejects set-returning functions in scalar contexts
- Optimizes constant arguments by pre-evaluating them during setup rather than at every execution
- Selects different opcodes based on function strictness and statistics tracking level to optimize runtime performance
- The function is static and only used internally within the expression evaluation system