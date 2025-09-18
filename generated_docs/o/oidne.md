# oidne

## Location
src/backend/utils/adt/oid.c: 281 - 289

## Overview
A PostgreSQL function that tests inequality between two Oid (Object Identifier) values, returning a boolean result indicating whether the two Oids are not equal.

## Definition
```c
Datum oidne(PG_FUNCTION_ARGS)
```

## Detailed Description
The oidne function is a PostgreSQL system function that implements the inequality operator (!=, <>) for the Oid data type. It follows the standard PostgreSQL function calling convention using PG_FUNCTION_ARGS to receive parameters and returns a Datum. The function extracts two Oid arguments using PG_GETARG_OID macros and performs a simple inequality comparison using the != operator. The result is returned as a boolean value using PG_RETURN_BOOL. This function is typically invoked through SQL queries when comparing Oid values with the != or <> operators.

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
This function is part of PostgreSQL's operator infrastructure and is typically not called directly in C code but rather invoked through SQL expressions using the != or <> operators on Oid values. The function follows PostgreSQL's standard function calling convention and is registered in the system catalogs as the implementation for Oid inequality comparison.