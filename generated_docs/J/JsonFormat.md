# JsonFormat

## Location
src/include/nodes/primnodes.h: 1648 - 1654

## Overview
JsonFormat represents a JSON FORMAT clause in PostgreSQL, specifying the format type and encoding for JSON operations and expressions.

## Definition
```c
typedef enum JsonEncoding
{
    JS_ENC_DEFAULT,     /* unspecified */
    JS_ENC_UTF8,
    JS_ENC_UTF16,
    JS_ENC_UTF32,
} JsonEncoding;

typedef enum JsonFormatType
{
    JS_FORMAT_DEFAULT,  /* unspecified */
    JS_FORMAT_JSON,     /* FORMAT JSON [ENCODING ...] */
    JS_FORMAT_JSONB,    /* implicit internal format for RETURNING jsonb */
} JsonFormatType;

typedef struct JsonFormat
{
    NodeTag     type;
    JsonFormatType format_type; /* format type */
    JsonEncoding encoding;      /* JSON encoding */
    ParseLoc    location;       /* token location, or -1 if unknown */
} JsonFormat;
```

## Detailed Description
JsonFormat is a node structure that represents JSON FORMAT clauses used in various JSON functions and expressions in PostgreSQL. It specifies how JSON data should be formatted and encoded. The structure supports different JSON format types (default, JSON, or JSONB) and various encoding options (default, UTF8, UTF16, UTF32).

This structure is commonly used in JSON functions like JSON_QUERY, JSON_VALUE, and other SQL/JSON operations where the output format and encoding need to be specified. The format_type field determines whether the output should be in JSON text format or JSONB binary format, while the encoding field specifies the character encoding for JSON text output.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonFormat node
- `format_type`: JsonFormatType enum specifying the JSON format (default, JSON, or JSONB)
- `encoding`: JsonEncoding enum specifying the character encoding (default, UTF8, UTF16, or UTF32)
- `location`: Parse location in the original SQL text, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - JsonFormatType (enum)
  - JsonEncoding (enum)
  - ParseLoc
  - NodeTag

- Called from (representative examples):
  - makeJsonFormat (makefuncs.c:896)
  - makeJsonValueExpr (makefuncs.c:911)
  - makeJsonIsPredicate (makefuncs.c:958)
  - transformJsonParseArg (parse_expr.c:4040)
  - transformJsonFuncExpr (parse_expr.c:4309)
  - get_json_format (ruleutils.c:11297)

## Notes and Other Information
- JsonFormat is a standalone node type, not inheriting from the Expr hierarchy
- The structure is used in conjunction with various JSON-related expression nodes like JsonValueExpr, JsonIsPredicate, and JsonExpr
- JS_FORMAT_DEFAULT indicates that no explicit format was specified in the SQL
- JS_FORMAT_JSON specifies explicit JSON text output with optional encoding
- JS_FORMAT_JSONB is used internally for RETURNING jsonb operations
- JS_ENC_DEFAULT allows the system to choose appropriate encoding
- The encoding field is primarily relevant for JSON text output, not for JSONB format
- JsonFormat nodes are created by parser functions and used throughout the JSON expression processing pipeline
- The structure supports SQL standard JSON syntax for format and encoding specifications
- Used extensively in SQL/JSON path expressions and JSON constructor functions