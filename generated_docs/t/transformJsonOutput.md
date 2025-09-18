# transformJsonOutput

## Location
src/backend/parser/parse_expr.c: 3500 - 3546

## Overview
Transforms a JSON output clause from parse tree representation to an internal JsonReturning structure, handling type resolution, format assignment, and validation.

## Definition
```c
static JsonReturning *transformJsonOutput(ParseState *pstate, const JsonOutput *output,
                                         bool allow_format)
```

## Detailed Description
This function converts a parsed JSON output clause into a JsonReturning structure used internally by PostgreSQL's JSON functionality. It performs several key operations:

1. **Default Handling**: When no output clause is specified, creates a default JsonReturning with default format and invalid type identifiers
2. **Type Resolution**: Resolves the specified type name to get the actual type OID and modifier using the type system
3. **Type Validation**: Ensures the target type is not a SETOF type or pseudo-type, which are not supported in SQL/JSON functions
4. **Format Assignment**: Either assigns appropriate default formats (JSONB for JSONB types, JSON otherwise) or validates user-specified formats

The function integrates with PostgreSQL's type system and ensures that JSON output specifications conform to supported patterns.

## Parameters / Member Variables
- `pstate`: ParseState pointer for parser context and error reporting
- `output`: JsonOutput pointer containing the parsed output clause, or NULL for default behavior
- `allow_format`: Boolean indicating whether custom format specifications should be allowed for non-string types

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - makeJsonFormat
  - copyObject
  - typenameTypeIdAndMod
  - get_typtype
  - checkJsonOutputFormat
  - ereport (for error reporting)
- Called from (representative examples):
  - transformJsonConstructorOutput
  - transformJsonReturning
  - transformJsonSerializeExpr
  - transformJsonFuncExpr

## Notes and Other Information
- This is a static function used internally within PostgreSQL's JSON expression parsing
- Automatically selects JSONB format for JSONB return types and JSON format for other types when no explicit format is specified
- Enforces restrictions on SETOF and pseudo-types which are not supported in SQL/JSON contexts
- The function handles both explicit output clauses and implicit default behavior
- Part of PostgreSQL's SQL/JSON standard compliance implementation