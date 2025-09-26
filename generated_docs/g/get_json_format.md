# get_json_format

## Location
[src/backend/utils/adt/ruleutils.c:11297-11321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11297-L11321)

## Overview
A static helper function within the rule decompilation system that parses back a JsonFormat node into its SQL text representation, handling FORMAT and ENCODING clauses.

## Definition
```c
static void get_json_format(JsonFormat *format, StringInfo buf)
```

## Detailed Description
This function is responsible for decompiling JsonFormat nodes back to their SQL text form during rule decompilation. JsonFormat nodes represent the FORMAT and ENCODING specifications that can be used with JSON functions in PostgreSQL's SQL/JSON support.

The function handles two main aspects:
1. **Format Type**: Determines whether to output "FORMAT JSON" or "FORMAT JSONB" based on the format_type field, or omits the FORMAT clause entirely if it's the default
2. **Encoding**: Appends an ENCODING clause when a non-default encoding is specified (UTF16, UTF32, or UTF8)

The function only outputs clauses when they differ from defaults, keeping the decompiled SQL concise while preserving semantic meaning. It directly appends formatted strings to the provided StringInfo buffer rather than returning values.

## Parameters / Member Variables
- `format`: Pointer to a JsonFormat structure containing format type and encoding information
- `buf`: StringInfo buffer where the formatted output is appended

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoString (appends string to StringInfo buffer)
  - appendStringInfo (appends formatted string to StringInfo buffer)
  - JS_FORMAT_DEFAULT (enum value for default format type)
  - JS_FORMAT_JSONB (enum value for JSONB format type)
  - JS_ENC_DEFAULT (enum value for default encoding)
  - JS_ENC_UTF16, JS_ENC_UTF32 (enum values for UTF16/UTF32 encodings)
- Called from (representative examples):
  - get_rule_expr (general expression decompilation)
  - get_json_returning (JSON returning clause decompilation)

## Notes and Other Information
- This is a static function local to ruleutils.c, part of the internal decompilation infrastructure
- Part of PostgreSQL's SQL/JSON standard compliance implementation
- Only outputs FORMAT and ENCODING clauses when they differ from defaults, improving readability
- The function assumes UTF8 as the fallback encoding name for any encoding that's not UTF16 or UTF32
- Located in src/backend/utils/adt/ruleutils.c:11297-11321