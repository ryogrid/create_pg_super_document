# transformJsonConstructorOutput

## Location
src/backend/parser/parse_expr.c: 3547 - 3589

## Overview
Transforms JSON output clause specifically for JSON constructor functions, with automatic type derivation based on constructor argument types when no explicit return type is specified.

## Definition
```c
static JsonReturning *transformJsonConstructorOutput(ParseState *pstate, JsonOutput *output,
                                                     List *args)
```

## Detailed Description
This function extends the basic JSON output transformation with special logic for JSON constructor functions (such as JSON_OBJECT, JSON_ARRAY, JSON_OBJECTAGG, JSON_ARRAYAGG). The key feature is automatic return type inference:

1. **Base Transformation**: Calls transformJsonOutput with allow_format=true to handle standard output clause processing
2. **Type Inference Logic**: When no explicit return type is specified (returning->typid is invalid), examines the constructor's argument types
3. **JSONB Priority**: If any argument is of JSONB type, the entire result becomes JSONB with JSONB format
4. **JSON Default**: Otherwise defaults to JSON type with JSON format (deviating from SQL standard which specifies TEXT)

This approach ensures that JSONB "stickiness" is preserved - if any input to a JSON constructor is JSONB, the output maintains JSONB type to prevent data loss.

## Parameters / Member Variables
- `pstate`: ParseState pointer for parser context and error reporting
- `output`: JsonOutput pointer containing the parsed output clause specification  
- `args`: List of expression nodes representing the constructor's arguments for type inference

## Dependencies
- Functions called/Symbols referenced:
  - transformJsonOutput
  - OidIsValid
  - lfirst
  - exprType
- Called from (representative examples):
  - transformJsonObjectConstructor
  - transformJsonObjectAgg  
  - transformJsonArrayAgg
  - transformJsonArrayConstructor

## Notes and Other Information
- This is a static function specialized for JSON constructor function parsing
- Implements "JSONB stickiness" - any JSONB input results in JSONB output
- Deviates from SQL/JSON standard by defaulting to JSON type instead of TEXT when no JSONB arguments are present
- The type inference scans arguments until a JSONB type is found, optimizing for the common case
- Sets typmod to -1 (no type modifier) for inferred types
- Part of PostgreSQL's comprehensive JSON constructor function support