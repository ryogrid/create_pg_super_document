# simple_quote_literal

## Location
[src/backend/utils/adt/ruleutils.c:11463-11489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11463-L11489)

## Overview
A static utility function that formats a C string as a SQL string literal by properly escaping characters and appending the result to a StringInfo buffer.

## Definition
```c
static void simple_quote_literal(StringInfo buf, const char *val)
```

## Detailed Description
This function converts a C string into a properly formatted SQL string literal according to PostgreSQL's SQL standards. It handles character escaping based on the current setting of standard_conforming_strings, ensuring that the resulting literal can be safely used in SQL contexts.

The function processes each character in the input string and applies appropriate escaping rules:
- Characters that need doubling (according to SQL_STR_DOUBLE macro) are duplicated
- The entire string is wrapped in single quotes
- No E prefix is used, relying instead on the standard_conforming_strings setting

This function is part of PostgreSQL's rule deparsing infrastructure, used when converting internal representations back to readable SQL text.

## Parameters / Member Variables
- `buf`: A StringInfo buffer where the formatted SQL literal will be appended
- `val`: A null-terminated C string containing the text to be formatted as a SQL literal

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (for adding single characters to the buffer)
  - SQL_STR_DOUBLE (macro for determining which characters need doubling)
- Global variables referenced:
  - standard_conforming_strings (controls escaping behavior)
- Called from (representative examples):
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md) (for trigger definitions)
  - [pg_get_functiondef](../p/pg_get_functiondef.md) (for function definitions)
  - [get_utility_query_def](../g/get_utility_query_def.md) (for utility statements)
  - [get_rule_expr](../g/get_rule_expr.md) (for rule expressions)
  - [get_const_expr](../g/get_const_expr.md) (for constant expressions)
  - [get_reloptions](../g/get_reloptions.md) (for relation options)

## Notes and Other Information
- This is a static function within ruleutils.c, used exclusively for rule deparsing operations
- The function respects the standard_conforming_strings GUC setting to determine proper escaping behavior
- Unlike some other quoting functions, this does not use the E (escape) syntax, making it suitable for contexts where standard SQL string literals are preferred
- The function is widely used throughout the rule deparsing system for any context where string literals need to be generated
- Character doubling is handled by the SQL_STR_DOUBLE macro, which abstracts the logic for determining which characters require escaping