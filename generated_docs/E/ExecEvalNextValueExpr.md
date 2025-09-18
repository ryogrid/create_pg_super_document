# ExecEvalNextValueExpr

## Location
src/backend/executor/execExprInterp.c: 2717 - 2742

## Overview
ExecEvalNextValueExpr evaluates a NextValueExpr by retrieving the next value from a sequence and converting it to the appropriate data type.

## Definition
```c
void ExecEvalNextValueExpr(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
This function implements the evaluation of sequence nextval expressions in PostgreSQL. It calls nextval_internal to get the next value from the specified sequence, then converts that value to the appropriate integer type (int2, int4, or int8) based on the sequences data type. The function is part of the expression evaluation framework and is called during query execution when a NEXTVAL expression needs to be evaluated.

The function handles three supported sequence types:
- INT2OID: 16-bit integers (smallint)
- INT4OID: 32-bit integers (integer) 
- INT8OID: 64-bit integers (bigint)

Any unsupported sequence type will cause an error.

## Parameters / Member Variables
- `state`: ExprState pointer containing the expression evaluation state
- `op`: ExprEvalStep pointer containing the operation details including:
  - `op->d.nextvalueexpr.seqid`: The OID of the sequence to get the next value from
  - `op->d.nextvalueexpr.seqtypid`: The data type OID of the sequence
  - `op->resvalue`: Pointer to store the resulting Datum value
  - `op->resnull`: Pointer to store the null indicator (always set to false)

## Dependencies
- Functions called/Symbols referenced:
  - nextval_internal: Core function to get next sequence value
  - Int16GetDatum: Convert int16 to Datum
  - Int32GetDatum: Convert int32 to Datum (implicitly used)
  - Int64GetDatum: Convert int64 to Datum
  - ExprEvalStep: Structure containing evaluation step details
- Called from (representative examples):
  - ExecInterpExpr: Main expression interpreter loop
  - FunctionReturningBool: JIT compilation type definitions

## Notes and Other Information
- The function always sets *op->resnull to false since sequence nextval operations never return NULL
- Sequence values are internally represented as int64, then cast to the target type
- This is part of the step-based expression evaluation system introduced for performance
- The function assumes the sequence exists and is accessible; access control is handled elsewhere