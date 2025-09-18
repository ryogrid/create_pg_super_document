# compatible_oper

## Location
src/backend/parser/parse_oper.c: 450 - 486

## Overview
A more restrictive operator resolution function that finds binary operators without requiring run-time type coercion, accepting only exact or binary-compatible operators.

## Definition
```c
Operator compatible_oper(ParseState *pstate, List *op, Oid arg1, Oid arg2, bool noError, int location)
```

## Detailed Description
The `compatible_oper` function provides stricter operator resolution than the standard `oper` function. It first uses `oper` to find the best available operator match, then verifies that the found operator can accept the input data types without requiring run-time type coercion. Only operators that are exactly compatible or binary-compatible with the input types are accepted. If the operator requires coercion, it is rejected and an error is reported (unless noError is true). This function is essential when exact type matching is required for performance or semantic reasons.

## Parameters / Member Variables
- `pstate`: Parse state context for error reporting (can be NULL)
- `op`: List containing the operator name components (namespace, operator symbol)
- `arg1`: Object identifier of the first operand's data type
- `arg2`: Object identifier of the second operand's data type
- `noError`: If true, return NULL on failure; if false, raise an error
- `location`: Source location for error reporting (-1 if not available)

## Dependencies
- Functions called/Symbols referenced:
  - oper (performs initial operator resolution)
  - IsBinaryCoercible (checks if types are binary-compatible without coercion)
  - Form_pg_operator (operator catalog form structure)
  - op_signature_string (generates operator signature for error messages)
  - ReleaseSysCache (releases syscache entry when operator is rejected)
  - ereport, errcode, errmsg, parser_errposition (error reporting)
- Called from (representative examples):
  - compatible_oper_opid (wrapper function that returns just the operator OID)

## Notes and Other Information
- More restrictive than oper() - rejects operators requiring type coercion
- Returns NULL if noError is true and no compatible operator found
- Properly manages syscache entries by releasing rejected operators
- Provides detailed error messages including operator signature
- Critical for scenarios where performance or semantic requirements mandate exact type matching
- Located in src/backend/parser/parse_oper.c:450-486
- Used when binary compatibility is specifically required over general coercion compatibility