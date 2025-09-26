# get_json_behavior

## Location
[src/backend/utils/adt/ruleutils.c:8877-8914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L8877-L8914)

## Overview
Formats and outputs JSON behavior specifications (NULL, ERROR, EMPTY, etc.) for SQL/JSON functions in PostgreSQL rule decompilation.

## Definition

```c
static void
get_json_behavior(JsonBehavior *behavior, deparse_context *context,
				  const char *on)
```
## Detailed Description
This function converts JsonBehavior structures into their corresponding SQL text representation for JSON path expressions. It maintains a static array of behavior names that directly corresponds to the JsonBehaviorType enumeration members. The function handles all JSON behavior types including NULL, ERROR, EMPTY, TRUE, FALSE, UNKNOWN, EMPTY ARRAY, EMPTY OBJECT, and DEFAULT behaviors.

For DEFAULT behaviors, the function additionally processes the associated expression using get_rule_expr. The function concludes by appending the "ON" clause with the specified context (such as "ON ERROR" or "ON EMPTY").

The implementation includes bounds checking to ensure the behavior type is valid, throwing an error for invalid behavior types to maintain system integrity.

## Parameters / Member Variables
- : JsonBehavior structure containing the behavior type and optional default expression
- : Deparse context containing the output buffer and formatting settings  
- : String specifying the ON clause context (e.g., "ERROR", "EMPTY")

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro to get array length)
  - elog (error logging function) 
  - [appendStringInfoString](../a/appendStringInfoString.md) (appends string to buffer)
  - [appendStringInfo](../a/appendStringInfo.md) (formatted string append to buffer)
  - [get_rule_expr](get_rule_expr.md) (processes default expressions for JSON_BEHAVIOR_DEFAULT)
  - JSON_BEHAVIOR_DEFAULT (enum value for default behavior type)
  - [JsonBehavior](../J/JsonBehavior.md) (structure type for JSON behaviors)
- Called from (representative examples):
  - [get_json_expr_options](get_json_expr_options.md) (for JSON expression option formatting)
  - [get_json_table](get_json_table.md) (for JSON table function formatting)

## Notes and Other Information
- This is a static function within ruleutils.c, specifically for SQL/JSON functionality
- The behavior_names array must maintain correspondence with JsonBehaviorType enum order
- Only DEFAULT behavior types require additional expression processing
- The function enforces type safety through bounds checking on behavior types
- Essential for proper SQL/JSON syntax generation in rule decompilation
- Location: src/backend/utils/adt/ruleutils.c:8877-8914