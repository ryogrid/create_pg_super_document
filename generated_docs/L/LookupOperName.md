# LookupOperName

## Location
src/backend/parser/parse_oper.c: 99 - 132

## Overview
Looks up an operator by name and exact input datatypes, with support for schema-qualified names and error handling options.

## Definition


## Detailed Description
LookupOperName searches for an operator given its name (possibly schema-qualified) and the exact OIDs of its left and right operand types. The function uses the current namespace search path when the operator name is not schema-qualified. It provides flexible error handling - either returning InvalidOid or raising an error when the operator is not found. The function specifically checks for unsupported postfix operators and provides detailed error messages with position information.

## Parameters / Member Variables
- : Parse state for error reporting context (can be NULL)
- : List containing the operator name (possibly schema-qualified)
- : OID of the left operand type (use InvalidOid for prefix operators)
- : OID of the right operand type
- : If true, return InvalidOid on failure; if false, raise an error
- : Token location for error reporting (use -1 if not available)

## Dependencies
- Functions called/Symbols referenced:
  - OpernameGetOprid
  - op_signature_string
- Called from (representative examples):
  - AggregateCreate
  - OperatorLookup
  - DefineOpClass
  - LookupOperWithArgs

## Notes and Other Information
- Returns InvalidOid for failed lookups when noError is true
- Explicitly rejects postfix operators with a syntax error
- Uses exact type matching only - no type coercion is performed
- Error messages include the full operator signature for better diagnostics
- Part of the PostgreSQL parser's operator resolution system