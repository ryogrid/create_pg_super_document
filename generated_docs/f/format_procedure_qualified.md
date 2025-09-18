# format_procedure_qualified

## Location
src/backend/utils/adt/regproc.c: 305 - 325

## Overview
Converts a procedure OID to its fully qualified string representation, always including schema qualification regardless of the current search path.

## Definition
```c
char *format_procedure_qualified(Oid procedure_oid)
```

## Detailed Description
The `format_procedure_qualified` function provides a wrapper around `format_procedure_extended` that forces schema qualification in the output. Unlike `format_procedure` which may omit schema names for procedures in the current search path, this function always includes the schema name (e.g., "pg_catalog.array_length(anyarray, integer)" instead of just "array_length(anyarray, integer)").

This function is particularly useful when you need unambiguous procedure references that will work regardless of the current schema search path setting. It ensures that the formatted procedure name can be safely used in contexts where schema qualification is required for correctness.

The function uses the `FORMAT_PROC_FORCE_QUALIFY` flag (0x02) to force qualification of the procedure name with its schema.

## Parameters / Member Variables
- `procedure_oid`: The OID of the procedure to format with forced qualification

## Dependencies
- Functions called/Symbols referenced:
  - `[format_procedure_extended](format_procedure_extended.md)`: Core formatting function with extended options
  - `FORMAT_PROC_FORCE_QUALIFY`: Flag constant to force schema qualification (0x02)
- Called from (representative examples):
  - `FORMAT_OPERATOR_FORCE_QUALIFY`: Macro reference in regproc header

## Notes and Other Information
- Always returns schema-qualified procedure names regardless of search_path
- Useful for generating procedure references that are context-independent
- Returns a palloc'd string that must be managed within PostgreSQL's memory context
- Part of PostgreSQL's object formatting system for generating unambiguous object references
- The qualification ensures the procedure name will resolve correctly even if search_path changes
- Commonly used when storing procedure references that need to be persistent and unambiguous