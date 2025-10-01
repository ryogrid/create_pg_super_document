# get_json_table_nested_columns

## Location
[src/backend/utils/adt/ruleutils.c:11714-11745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11714-L11745)

## Overview
Recursively parses and formats nested JSON_TABLE column specifications for SQL query deparsing, handling both path scans and sibling joins in JSON table expressions.

## Definition

```c
static void
get_json_table_nested_columns(TableFunc *tf, JsonTablePlan *plan,
							  deparse_context *context, bool showimplicit,
							  bool needcomma)
```
## Detailed Description
This function is part of PostgreSQL's rule deparsing system, specifically for reconstructing JSON_TABLE expressions from their internal representation. It recursively processes nested column structures in JSON table functions, handling two main types of plans:

1. **JsonTablePathScan**: Represents a NESTED PATH clause with associated columns
2. **JsonTableSiblingJoin**: Represents multiple nested paths that need to be joined

The function formats the output with proper SQL syntax, including comma separation, NESTED PATH keywords, and quoted identifiers. It calls itself recursively to handle complex nested structures and delegates column formatting to .

## Parameters / Member Variables
- : TableFunc structure containing the JSON table function definition
- : JsonTablePlan that specifies the execution plan for the nested columns
- : deparse_context containing the output buffer and formatting state
- : Boolean flag indicating whether to show implicit column specifications
- : Boolean flag indicating whether a comma separator is needed before output

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - castNode (macro for safe type casting)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - [get_const_expr](get_const_expr.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [get_json_table_columns](get_json_table_columns.md)
  - [get_json_table_nested_columns](get_json_table_nested_columns.md) (recursive call)
- Called from (representative examples):
  - [get_json_table_columns](get_json_table_columns.md)

## Notes and Other Information
- This is a static function used internally by the rule deparsing system
- The function handles recursive data structures, making it capable of processing arbitrarily nested JSON table column specifications
- Part of the broader JSON_TABLE functionality introduced in PostgreSQL for parsing JSON data into relational format
- The function preserves the original SQL syntax structure when reconstructing queries from internal representations

## Simplified Source

```c
static void
get_json_table_nested_columns(TableFunc *tf, JsonTablePlan *plan,
                              deparse_context *context, bool showimplicit,
                              bool needcomma)
{
    if (IsA(plan, JsonTablePathScan)) {
        // Handle NESTED PATH clause
        JsonTablePathScan *scan = castNode(JsonTablePathScan, plan);

        if (needcomma)
            appendStringInfoChar(context->buf, ',');

        appendStringInfoChar(context->buf, ' ');
        appendContextKeyword(context, "NESTED PATH ", 0, 0, 0);

        // Add the path expression and alias
        get_const_expr(scan->path->value, context, -1);
        appendStringInfo(context->buf, " AS %s", quote_identifier(scan->path->name));

        // Generate column specifications for this nested path
        get_json_table_columns(tf, scan, context, showimplicit);
    }
    else if (IsA(plan, JsonTableSiblingJoin)) {
        // Handle multiple nested paths (sibling joins)
        JsonTableSiblingJoin *join = (JsonTableSiblingJoin *) plan;

        // Recursively process left and right nested plans
        get_json_table_nested_columns(tf, join->lplan, context, showimplicit, needcomma);
        get_json_table_nested_columns(tf, join->rplan, context, showimplicit, true);
    }
}
```