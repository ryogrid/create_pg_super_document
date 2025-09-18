# binary_oper_exact

## Location
[src/backend/parser/parse_oper.c:262-311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L262-L311)

## Overview
A static helper function that searches for an exact operator match given specific operand types, with special handling for unknown literals and domain types.

## Definition


## Detailed Description
The  function performs an "exact" match search for binary operators based on the specified operand types. It implements intelligent type resolution by treating unknown literals as having the same type as the other operand when one operand type is unknown. Additionally, it considers domain types that may need to be reduced to their base types to find an exact match. The function first attempts a direct operator lookup, and if that fails with unknown types involved, it tries again using base types.

## Parameters / Member Variables
- : List containing the operator name components (e.g., namespace and operator symbol)
- : Object identifier (Oid) of the first operand's data type  
- : Object identifier (Oid) of the second operand's data type

## Dependencies
- Functions called/Symbols referenced:
  - [OpernameGetOprid](../O/OpernameGetOprid.md) (called twice for operator lookup)
  - [getBaseType](../g/getBaseType.md) (to resolve domain types to base types)
  - FuncDetailCode (referenced but usage context unclear from this snippet)
- Called from (representative examples):
  - [oper](../o/oper.md) (main operator resolution function)

## Notes and Other Information
- Returns InvalidOid when no exact match is found
- Uses UNKNOWNOID constant to identify unspecified types
- Implements PostgreSQL's type coercion logic for unknown literals
- Part of PostgreSQL's operator resolution system in the parser
- Located in src/backend/parser/parse_oper.c:262-311