# makeNullConst

## Location
src/backend/nodes/makefuncs.c: 386 - 405

## Overview
Creates a Const node representing a NULL value of a specified data type, providing a convenient wrapper around makeConst for NULL constants.

## Definition
```c
Const *makeNullConst(Oid consttype, int32 consttypmod, Oid constcollid)
```

## Detailed Description
The `makeNullConst` function is a convenience wrapper that creates a Const node representing a NULL value of a specified PostgreSQL data type. Rather than requiring the caller to look up storage properties of the data type, this function automatically retrieves the necessary type information (length and pass-by-value flag) and creates the appropriate NULL constant.

The function uses the PostgreSQL type system to determine how the data type is stored internally, then delegates to `makeConst` with appropriate parameters to create a NULL constant. This is commonly used throughout the system when NULL values of specific types need to be created during query processing, optimization, or execution.

## Parameters / Member Variables
- `consttype`: OID of the data type for which to create a NULL constant
- `consttypmod`: Type modifier for the data type (additional type information like precision/scale)
- `constcollid`: OID of the collation for collatable types (InvalidOid if not applicable)

## Dependencies
- Functions called/Symbols referenced:
  - get_typlenbyval (retrieves type storage properties from system catalog)
  - makeConst (creates the actual Const node)
  - Datum (PostgreSQL's generic value type)
- Called from (representative examples):
  - ATExecAddColumn (DDL commands)
  - ExecInitExprRec (expression execution)
  - eval_const_expressions_mutator (optimizer)
  - coerce_record_to_complex (parser)
  - rewriteValuesRTE (rewriter)

## Notes and Other Information
- This is a convenience function that saves callers from having to look up type storage properties manually  
- Always creates a NULL constant with `constisnull = true` and `constvalue = (Datum) 0`
- Automatically determines the correct `constlen` and `constbyval` values based on the data type
- Widely used throughout the system for creating typed NULL constants during query processing
- More convenient than calling makeConst directly when creating NULL values
- Located in src/backend/nodes/makefuncs.c:386-405