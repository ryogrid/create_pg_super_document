# get_const_expr

## Location
[src/backend/utils/adt/ruleutils.c:11135-11264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11135-L11264)

## Overview
Converts Const nodes into their appropriate string representations with intelligent type labeling, handling NULL values, numeric constants, booleans, and general literals while managing collation information.

## Definition
```c
static void get_const_expr(Const *constval, deparse_context *context, int showtype)
```

## Detailed Description
This function generates string representations of constant values from Const nodes, applying sophisticated logic to determine when type casting ('::typename') decoration is needed. For NULL values, it always includes type information to prevent parsing ambiguity. For numeric types, it uses specialized formatting: INT4 constants avoid quotes unless negative (to handle operator precedence), NUMERIC values are formatted as unquoted floats when possible, and BOOLOID values are converted to 'true'/'false'. The function synchronizes with parser behavior to determine implicit typing, ensuring that constants will be correctly interpreted when the SQL is re-parsed. It also handles collation information when the constant's collation differs from the type's default.

## Parameters / Member Variables
- `constval`: Pointer to the Const node containing the constant value and metadata to be formatted
- `context`: Pointer to deparse_context containing deparsing state, buffer, and configuration
- `showtype`: Integer flag controlling type decoration: -1 (never show), 0 (show when needed), +1 (always show)

## Dependencies
- Functions called/Symbols referenced:
  - getTypeOutputInfo
  - OidOutputFunctionCall
  - format_type_with_typemod
  - get_const_collation
  - simple_quote_literal
  - appendStringInfo functions
- Types referenced:
  - Const
  - deparse_context
  - Various type OIDs (INT4OID, NUMERICOID, BOOLOID, UNKNOWNOID)
- Called from (representative examples):
  - get_rule_expr
  - get_coercion_expr
  - get_rule_sortgroupclause
  - get_json_path_spec
  - get_range_partbound_string

## Notes and Other Information
The function implements careful synchronization with PostgreSQL's parser (especially make_const) to ensure that the generated constants will be interpreted with the correct types during re-parsing. Special handling exists for negative integers to avoid operator precedence issues, and for numeric values that might look like floats versus integers. The showtype parameter provides fine-grained control over type decoration, with -1 typically used when the caller will provide type information separately. Collation handling is coordinated with the showtype flag to avoid malformed output where collation clauses might conflict with type casting syntax.