# ExecEvalConstraintCheck

## Location
[src/backend/executor/execExprInterp.c:3866-3885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3866-L3885)

## Overview
Evaluates a CHECK domain constraint, raising an error if the constraint expression evaluates to false.

## Definition
void ExecEvalConstraintCheck(ExprState *state, ExprEvalStep *op)

## Detailed Description
This function enforces CHECK constraints on domain types during expression evaluation. It examines the result of a previously evaluated constraint expression and raises a CHECK_VIOLATION error if the constraint is not satisfied (i.e., the check expression evaluates to false and is not null).

The function is part of PostgreSQL's domain constraint enforcement mechanism, working alongside ExecEvalConstraintNotNull to ensure that values assigned to domain types comply with all defined constraints during runtime expression evaluation.

The constraint check follows SQL semantics where:
- NULL constraint results are considered to pass (constraint satisfied)
- FALSE constraint results trigger a violation error
- TRUE constraint results pass normally

## Parameters / Member Variables
- : Expression state context (unused in this function)
- : Expression evaluation step containing domain constraint information including the check result value, null status, constraint name, domain type, and error context

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetBool](../D/DatumGetBool.md)
  - errsave
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [format_type_be](../f/format_type_be.md)
  - [errdomainconstraint](../e/errdomainconstraint.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- The function assumes the constraint expression has already been evaluated and its result is stored in op->d.domaincheck.checkvalue
- NULL constraint results are treated as passing (follows SQL standard three-valued logic)
- Error messages include both the domain type name and specific constraint name for debugging
- Uses errsave for error reporting which allows for soft error handling in appropriate contexts
- Part of the broader domain constraint evaluation system in PostgreSQL's expression interpreter