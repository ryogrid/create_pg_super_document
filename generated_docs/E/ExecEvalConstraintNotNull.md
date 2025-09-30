# ExecEvalConstraintNotNull

## Location
[src/backend/executor/execExprInterp.c:3852-3865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3852-L3865)

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
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [format_type_be](../f/format_type_be.md)
  - [errdatatype](../e/errdatatype.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- This function only checks for null values; it doesn't validate the actual data content
- Uses errsave for error reporting which allows for soft error handling in appropriate contexts
- The error includes the domain type name formatted using format_type_be for user-friendly error messages
- Part of the domain constraint evaluation system alongside ExecEvalConstraintCheck for CHECK constraints

## Simplified Source

```c
void ExecEvalConstraintNotNull(ExprState *state, ExprEvalStep *op)
{
    // Check if result is null when NOT NULL constraint exists
    if (*op->resnull) {
        // Report NOT NULL constraint violation
        errsave((Node *) op->d.domaincheck.escontext,
                (errcode(ERRCODE_NOT_NULL_VIOLATION),
                 errmsg("domain %s does not allow null values",
                        format_type_be(op->d.domaincheck.resulttype)),
                 errdatatype(op->d.domaincheck.resulttype)));
    }
}
```