# ExecEvalConstraintNotNull

## Location
src/backend/executor/execExprInterp.c: 3852 - 3865

## Overview
Evaluates a NOT NULL domain constraint, raising an error if the evaluated expression result is null.

## Definition
void ExecEvalConstraintNotNull(ExprState *state, ExprEvalStep *op)

## Detailed Description
This function enforces NOT NULL constraints on domain types during expression evaluation. It checks if the result of the evaluated expression (stored in op->resnull) is null, and if so, raises a NOT_NULL_VIOLATION error with appropriate error context and type information.

The function is part of PostgreSQL's domain constraint enforcement mechanism, ensuring that values assigned to domain types comply with their NOT NULL constraints during runtime expression evaluation.

## Parameters / Member Variables
- : Expression state context (unused in this function)
- : Expression evaluation step containing domain constraint information including result nullness check, error context, and domain type information

## Dependencies
- Functions called/Symbols referenced:
  - errsave
  - errcode
  - errmsg
  - format_type_be
  - errdatatype
- Called from (representative examples):
  - ExecInterpExpr
  - FunctionReturningBool (via JIT compilation)

## Notes and Other Information
- This function only checks for null values; it doesn't validate the actual data content
- Uses errsave for error reporting which allows for soft error handling in appropriate contexts
- The error includes the domain type name formatted using format_type_be for user-friendly error messages
- Part of the domain constraint evaluation system alongside ExecEvalConstraintCheck for CHECK constraints