# escape_yaml

## Location
[src/backend/commands/explain.c:5299-5313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L5299-L5313)

## Overview
A utility function that escapes strings for safe inclusion in YAML output by delegating to JSON escaping rules.

## Definition
static void escape_yaml(StringInfo buf, const char *str)

## Detailed Description
This function handles string escaping for YAML format in PostgreSQL's EXPLAIN command output. Rather than implementing the complex YAML quoting rules defined in the YAML specification (sections 5.3 and 7.3.3), this function takes a pragmatic approach by reusing the existing JSON escaping functionality. This ensures that all strings are properly quoted and escaped, preventing issues with special characters, whitespace, or values that could be misinterpreted as YAML constants (like hexadecimal numbers or boolean values).

## Parameters / Member Variables
- buf: StringInfo buffer where the escaped string will be appended
- str: The input string to be escaped for YAML output

## Dependencies
- Functions called/Symbols referenced:
  - [escape_json](escape_json.md) (JSON string escaping function)
- Called from (representative examples):
  - [ExplainPropertyList](../E/ExplainPropertyList.md)
  - [ExplainPropertyListNested](../E/ExplainPropertyListNested.md)
  - [ExplainProperty](../E/ExplainProperty.md)
  - [ExplainDummyGroup](../E/ExplainDummyGroup.md)

## Notes and Other Information
- This is a static function only accessible within the explain.c file
- YAML is technically a superset of JSON, making JSON escaping rules safe for YAML
- The approach of quoting everything avoids the complexity of YAML's intricate quoting rules
- Prevents misinterpretation of string values as YAML constants or special types
- Part of PostgreSQL's EXPLAIN output formatting system for structured data formats