# get_json_returning

## Location
src/backend/utils/adt/ruleutils.c: 11322 - 11341

## Overview
A static helper function within the rule decompilation system that parses back a JsonReturning structure into its SQL RETURNING clause representation.

## Definition
```c
static void get_json_returning(JsonReturning *returning, StringInfo buf, bool json_format_by_default)
```

## Detailed Description
This function is responsible for decompiling JsonReturning structures back to their SQL text form during rule decompilation. The JsonReturning structure represents the RETURNING clause used in JSON functions to specify the data type and format of the returned value.

The function performs several key operations:
1. **Validity Check**: Returns early if no valid type OID is specified in the returning clause
2. **Type Information**: Appends the RETURNING keyword followed by the target data type, including any type modifiers
3. **Format Optimization**: Intelligently determines whether to include FORMAT clauses based on context and defaults

The function includes logic to avoid redundant FORMAT specifications: it only calls get_json_format() when either the json_format_by_default flag is false, or when the specified format differs from the expected default format for the return type (JSON for regular types, JSONB for JSONBOID).

## Parameters / Member Variables
- `returning`: Pointer to a JsonReturning structure containing type and format information
- `buf`: StringInfo buffer where the formatted RETURNING clause is appended  
- `json_format_by_default`: Boolean flag indicating whether JSON format should be assumed by default

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro to check if OID is valid)
  - appendStringInfo (appends formatted text to StringInfo buffer)
  - format_type_with_typemod (formats type name with type modifiers)
  - get_json_format (handles FORMAT and ENCODING clause decompilation)
  - JSONBOID (OID constant for JSONB type)
  - JS_FORMAT_JSON, JS_FORMAT_JSONB (enum values for JSON format types)
- Called from (representative examples):
  - get_rule_expr (general expression decompilation)
  - get_json_constructor_options (JSON constructor options decompilation)

## Notes and Other Information
- This is a static function local to ruleutils.c, part of the internal decompilation infrastructure
- Part of PostgreSQL's SQL/JSON standard implementation for RETURNING clauses
- Optimizes output by omitting redundant format specifications when they match expected defaults
- The function handles the special case where JSONB types should default to JSONB format
- Located in src/backend/utils/adt/ruleutils.c:11322-11341