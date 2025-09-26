# generate_operator_name

## Location
src/backend/utils/adt/ruleutils.c: 13032 - 13108

## Overview
Computes the name to display for an operator specified by OID, given that it is being called with the specified actual argument types.

## Definition
```c
static char *generate_operator_name(Oid operid, Oid arg1, Oid arg2)
```

## Detailed Description
This function generates a properly formatted operator name for display purposes, handling operator resolution ambiguity by considering argument types. The function determines whether schema-qualification is necessary based on whether the parser would be able to resolve the correct operator given just the unqualified operator name with the specified argument types.

The function supports both binary operators (oprkind = `b`) and left unary operators (oprkind = `l`). If schema-qualification is needed, it wraps the operator name in the `OPERATOR(schema.name)` syntax required for qualified operator usage in expressions.

The result includes all necessary quoting and schema-prefixing, plus the OPERATOR() decoration needed to use a qualified operator name in an expression.

## Parameters / Member Variables
- `operid`: The OID of the operator to generate a name for
- `arg1`: The OID of the first (left) argument type; pass InvalidOid for unused arg of a unary operator
- `arg2`: The OID of the second (right) argument type; pass InvalidOid for unused arg of a unary operator

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup)
  - oper (binary operator lookup)
  - left_oper (left unary operator lookup)  
  - makeString (string construction utility)
  - oprid (get operator OID from Operator)
  - get_namespace_name_or_temp (namespace name resolution)
  - quote_identifier (identifier quoting)
- Called from (representative examples):
  - get_oper_expr (operator expression formatting)
  - get_rule_expr (rule expression decompilation)
  - get_sublink_expr (sublink expression formatting)
  - pg_get_indexdef_worker (index definition formatting)

## Notes and Other Information
- The function uses operator resolution logic to determine if schema-qualification is necessary
- Only handles binary (b) and left unary (l) operators; right unary operators would cause an error
- Memory management is handled through StringInfo buffer that caller must free
- Part of the rule decompilation system used for displaying stored rules, views, and constraints