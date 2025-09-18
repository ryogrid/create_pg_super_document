# compatible_oper_opid

## Location
src/backend/parser/parse_oper.c: 487 - 517

## Overview
A convenience wrapper function that returns only the operator OID from compatible operator resolution, without requiring the caller to manage syscache entries.

## Definition
```c
Oid compatible_oper_opid(List *op, Oid arg1, Oid arg2, bool noError)
```

## Detailed Description
The `compatible_oper_opid` function is a simplified interface to `compatible_oper` that extracts and returns only the operator OID, automatically handling syscache management. It calls `compatible_oper` with NULL parse state and -1 location (since no error position reporting is needed), then extracts the operator OID using `oprid` and properly releases the syscache entry. This function is ideal for callers who only need the operator identifier and don't want to deal with syscache entry lifecycle management.

## Parameters / Member Variables
- `op`: List containing the operator name components (namespace, operator symbol)
- `arg1`: Object identifier of the first operand's data type
- `arg2`: Object identifier of the second operand's data type
- `noError`: If true, return InvalidOid on failure; if false, raise an error

## Dependencies
- Functions called/Symbols referenced:
  - compatible_oper (performs the actual operator resolution)
  - oprid (extracts operator OID from operator tuple)
  - ReleaseSysCache (releases syscache entry)
  - Operator (operator syscache entry type)
- Called from (representative examples):
  - ComputeIndexAttrs (index attribute computation)
  - addTargetToSortList (sort target list processing)

## Notes and Other Information
- Convenience wrapper that simplifies syscache management for callers
- Returns InvalidOid when no compatible operator is found and noError is true
- Automatically handles syscache entry release, preventing memory leaks
- Does not provide parse state context, so error reporting is less detailed
- More convenient than compatible_oper when only the operator OID is needed
- Located in src/backend/parser/parse_oper.c:487-517
- Part of PostgreSQL's operator resolution API family