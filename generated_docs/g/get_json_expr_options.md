# get_json_expr_options

## Location
src/backend/utils/adt/ruleutils.c: 8915 - 8955

## Overview
Formats common options for SQL/JSON functions (JSON_QUERY, JSON_VALUE, JSON_EXISTS, JSON_TABLE) including wrapper options, quote handling, and error/empty behaviors.

## Definition

```c
static void
get_json_expr_options(JsonExpr *jsexpr, deparse_context *context,
					  JsonBehaviorType default_behavior)
```
## Detailed Description
This function handles the deparsing of options that are common across multiple SQL/JSON functions. For JSON_QUERY operations, it processes wrapper options (WITH CONDITIONAL/UNCONDITIONAL WRAPPER or WITHOUT WRAPPER) and quote handling options (OMIT QUOTES or KEEP QUOTES). The function also manages ON EMPTY and ON ERROR behaviors, but only outputs them when they differ from the specified default behavior to avoid redundant SQL text.

The wrapper handling logic accounts for different JsonWrapperType values, treating both JSW_NONE and JSW_UNSPEC as equivalent for "WITHOUT WRAPPER" output. The function optimizes output by only including non-default behaviors, making the generated SQL more concise while maintaining semantic accuracy.

## Parameters / Member Variables
- : JsonExpr structure containing the JSON expression options to be formatted
- : Deparse context containing the output buffer and formatting settings
- : Default JsonBehaviorType used to determine when to omit standard behaviors

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoString (appends strings to the output buffer)
  - get_json_behavior (formats JSON behavior clauses for ON EMPTY/ON ERROR)
  - JSON_QUERY_OP (enum value for JSON query operations)
  - JSW_CONDITIONAL, JSW_UNCONDITIONAL, JSW_NONE, JSW_UNSPEC (JSON wrapper type enums)
  - JsonExpr (structure type for JSON expressions)
  - JsonBehaviorType (enum type for JSON behaviors)
- Called from (representative examples):
  - get_rule_expr (for general JSON expression formatting)
  - get_json_table_columns (for JSON table column formatting)

## Notes and Other Information
- This is a static function within ruleutils.c, specifically for SQL/JSON functionality  
- Only processes wrapper and quote options for JSON_QUERY operations
- Optimizes output by omitting default behaviors to reduce SQL verbosity
- Handles both explicit and unspecified wrapper types consistently
- Essential for proper SQL/JSON syntax reconstruction in rule decompilation
- Location: src/backend/utils/adt/ruleutils.c:8915-8955