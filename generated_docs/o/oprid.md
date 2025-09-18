# oprid

## Location
src/backend/parser/parse_oper.c: 238 - 244

## Overview
Extracts the operator OID from an operator tuple structure, providing a simple accessor function for operator identification.

## Definition
```c
Oid oprid(Operator op)
```

## Detailed Description
oprid is a utility function that extracts the OID from an operator tuple (HeapTuple). It accesses the pg_operator system catalog structure to retrieve the operator's unique identifier. This function serves as a clean abstraction layer over the low-level tuple access macros, making code more readable and maintainable when working with operator tuples from system catalog lookups.

## Parameters / Member Variables
- `op`: Operator tuple (HeapTuple) from pg_operator system catalog

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_operator
  - GETSTRUCT
- Called from (representative examples):
  - [inclusion_get_strategy_procinfo](../i/inclusion_get_strategy_procinfo.md)
  - [minmax_get_strategy_procinfo](../m/minmax_get_strategy_procinfo.md)
  - [OperatorIsVisible](../O/OperatorIsVisible.md)
  - [compatible_oper_opid](../c/compatible_oper_opid.md)
  - [make_op](../m/make_op.md)
  - [regoperout](../r/regoperout.md)
  - [dumpOpr](../d/dumpOpr.md)

## Notes and Other Information
- Simple accessor function providing abstraction over tuple structure access
- Widely used throughout PostgreSQL for operator identification and processing
- Essential for operator visibility checks, catalog output functions, and pg_dump
- Part of the operator management and system catalog interface
- Returns the same OID that would be used to look up the operator in pg_operator
- Used extensively in BRIN index processing and operator compatibility checks