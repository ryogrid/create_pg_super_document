# oidle

## Location
src/backend/utils/adt/oid.c: 299 - 307

## Overview
A PostgreSQL function that tests whether one Oid (Object Identifier) value is less than or equal to another, returning a boolean result indicating the comparison outcome.

## Definition
```c
Datum oidle(PG_FUNCTION_ARGS)
```

## Detailed Description
The oidle function is a PostgreSQL system function that implements the less-than-or-equal-to operator (<=) for the Oid data type. It follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS to receive parameters and returns a Datum. The function extracts two Oid arguments using PG_GETARG_OID macros and performs a simple less-than-or-equal-to comparison using the <= operator. The result is returned as a boolean value using PG_RETURN_BOOL. This function is typically invoked through SQL queries when comparing Oid values with the <= operator, enabling ordering operations and range queries on Oid columns.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1`: First Oid value (extracted via PG_GETARG_OID(0))
  - `arg2`: Second Oid value (extracted via PG_GETARG_OID(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro)
  - PG_RETURN_BOOL (macro)
- Called from (representative examples):
  - No direct references found (called through PostgreSQL function call mechanism)

## Notes and Other Information
This function is part of PostgreSQL's operator infrastructure and is typically not called directly in C code but rather invoked through SQL expressions using the <= operator on Oid values. The function follows PostgreSQL's standard function calling convention and is registered in the system catalogs as the implementation for Oid less-than-or-equal-to comparison. It enables sorting and ordering operations on Oid values in SQL queries, complementing the other comparison operators.