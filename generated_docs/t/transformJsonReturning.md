# transformJsonReturning

## Location
src/backend/parser/parse_expr.c: 4113 - 4152

## Overview
Transforms and validates the RETURNING clause of JSON expressions, creating default JSON output specifications when none are provided.

## Definition
```c
static JsonReturning *transformJsonReturning(ParseState *pstate, JsonOutput *output, const char *fname)
```

## Detailed Description
This function processes RETURNING clauses in JSON expressions such as JSON_VALUE() and JSON_QUERY(). When an output specification is provided, it validates that the return type is either JSON or JSONB, reporting detailed errors for invalid types. When no RETURNING clause is specified, it creates a default JsonReturning node with JSON type and default formatting. This ensures all JSON expressions have proper output type specifications.

## Parameters / Member Variables
- `pstate`: ParseState pointer containing parsing context and state information
- `output`: JsonOutput pointer specifying the desired output type and format (may be NULL)  
- `fname`: const char pointer to the function name for error reporting context

## Dependencies
- Functions called/Symbols referenced:
  - [transformJsonOutput](transformJsonOutput.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
  - [format_type_be](../f/format_type_be.md)
  - [parser_errposition](../p/parser_errposition.md)
  - makeNode
  - [makeJsonFormat](../m/makeJsonFormat.md)
- Called from (representative examples):
  - [transformJsonParseExpr](transformJsonParseExpr.md) (src/backend/parser/parse_expr.c:4159)
  - [transformJsonScalarExpr](transformJsonScalarExpr.md) (src/backend/parser/parse_expr.c:4208)

## Notes and Other Information
- Only accepts JSON and JSONB types in RETURNING clauses  
- Provides helpful error hints suggesting to use json or jsonb types
- Creates default JSON output with JS_FORMAT_JSON and JS_ENC_DEFAULT when no output is specified
- Uses function name in error messages for better user diagnostics
- Part of the SQL/JSON standard implementation ensuring consistent return type handling
- Default typmod is set to -1 (no specific type modifier)