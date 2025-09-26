# get_func_sql_syntax

## Location
[src/backend/utils/adt/ruleutils.c:10819-11070](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L10819-L11070)

## Overview
A comprehensive function that converts built-in SQL function calls with special syntax into their standard SQL string representation, handling functions like EXTRACT, OVERLAY, SUBSTRING, TRIM, and timezone operations.

## Definition
```c
static bool get_func_sql_syntax(FuncExpr *expr, deparse_context *context)
```

## Detailed Description
This function recognizes and deparses PostgreSQL built-in functions that have special SQL syntax rather than standard function call syntax. It uses a large switch statement based on the function OID to identify specific functions and generate their appropriate SQL representations. The function handles complex syntax like 'AT TIME ZONE', 'OVERLAPS', 'EXTRACT(...FROM...)', 'SUBSTRING(...FROM...FOR...)', 'TRIM(BOTH/LEADING/TRAILING...FROM...)', and many others. Each case constructs the proper SQL syntax with correct parentheses, keywords, and argument ordering. Returns true if the function was successfully deparsed, false if it's not a recognized special-syntax function.

## Parameters / Member Variables
- `expr`: Pointer to FuncExpr node containing the function call information to be deparsed
- `context`: Pointer to deparse_context containing deparsing state, buffer, and configuration for output formatting

## Dependencies
- Functions called/Symbols referenced:
  - [get_rule_expr](get_rule_expr.md)
  - [get_rule_expr_paren](get_rule_expr_paren.md)
  - TextDatumGetCString
  - [appendStringInfo](../a/appendStringInfo.md) functions
  - linitial, lsecond, lthird, lfourth (list access macros)
- Types referenced:
  - [FuncExpr](../F/FuncExpr.md)
  - [deparse_context](../d/deparse_context.md)
  - [Const](../C/Const.md)
  - Various F_* function OID constants
- Called from (representative examples):
  - [get_func_expr](get_func_expr.md)

## Notes and Other Information
This function is crucial for maintaining SQL standard compliance when deparsing query trees. Without it, special syntax functions would appear as regular function calls (e.g., 'extract(text, timestamp)' instead of 'EXTRACT(text FROM timestamp)'). The function covers a comprehensive set of SQL standard functions including temporal functions (timezone, extract), string functions (substring, overlay, trim, position), normalization functions, and XML functions. Each case carefully preserves the exact SQL syntax including proper keyword placement, parentheses, and argument ordering to ensure the output is valid SQL.