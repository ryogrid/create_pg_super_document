# get_json_table

## Location
[src/backend/utils/adt/ruleutils.c:11852-11920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11852-L11920)

## Overview
Reconstructs the complete JSON_TABLE function call syntax from its internal representation during SQL query deparsing, including document expression, path, passing clauses, columns, and error handling.

## Definition
```c
static void get_json_table(TableFunc *tf, deparse_context *context, bool showimplicit)
```

## Detailed Description
This function is the main entry point for deparsing JSON_TABLE expressions in PostgreSQL's rule system. It reconstructs the complete JSON_TABLE function call syntax including:

1. **Function signature**: Outputs "JSON_TABLE(" with proper formatting
2. **Document expression**: Processes the input JSON document expression
3. **Root path specification**: Handles the main PATH clause with alias
4. **PASSING clause**: Optionally processes parameter passing with variable bindings
5. **Column specifications**: Delegates to get_json_table_columns for detailed column processing
6. **Error handling**: Processes ON ERROR behavior clauses
7. **Proper formatting**: Handles indentation and pretty-printing

The function maintains the original SQL syntax structure while reconstructing from the parsed TableFunc representation, ensuring that the output is valid SQL that would parse back to the same internal structure.

## Parameters / Member Variables
- `tf`: TableFunc structure containing the complete JSON_TABLE definition including document expression, execution plan, column specifications, and options
- `context`: deparse_context containing the output buffer, formatting preferences, and indentation state
- `showimplicit`: Boolean flag indicating whether to show implicit specifications that could otherwise be omitted

## Dependencies
- Functions called/Symbols referenced:
  - castNode (safe type casting macro)
  - [appendStringInfoString](../a/appendStringInfoString.md), appendStringInfo, appendStringInfoChar
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - PRETTY_INDENT, PRETTYINDENT_VAR (formatting macros)
  - [get_rule_expr](get_rule_expr.md) (for expressions)
  - [get_const_expr](get_const_expr.md) (for constant values)
  - [quote_identifier](../q/quote_identifier.md) (for SQL identifier quoting)
  - forboth (macro for parallel list iteration)
  - lfirst_node (list access macro)
  - [get_json_table_columns](get_json_table_columns.md)
  - [get_json_behavior](get_json_behavior.md)
- Called from (representative examples):
  - [get_tablefunc](get_tablefunc.md)

## Notes and Other Information
- This is a static function used internally by the rule deparsing system
- The function handles optional PASSING clauses with parameter binding syntax
- Error behavior processing only outputs non-default error handling specifications
- Proper indentation and formatting support for readable SQL output
- Part of PostgreSQL's JSON_TABLE functionality for converting JSON to relational data
- The function preserves all semantic information needed to reconstruct the original query
- Coordinates with other JSON table functions to handle nested column structures

## Simplified Source

```c
static void get_json_table(TableFunc *tf, deparse_context *context, bool showimplicit) {
    StringInfo buf = context->buf;
    JsonExpr *jexpr = castNode(JsonExpr, tf->docexpr);
    JsonTablePathScan *root = castNode(JsonTablePathScan, tf->plan);

    appendStringInfoString(buf, "JSON_TABLE(");

    if (PRETTY_INDENT(context))
        context->indentLevel += PRETTYINDENT_VAR;

    appendContextKeyword(context, "", 0, 0, 0);

    // Output document expression
    get_rule_expr(jexpr->formatted_expr, context, showimplicit);

    appendStringInfoString(buf, ", ");

    // Output root path with alias
    get_const_expr(root->path->value, context, -1);
    appendStringInfo(buf, " AS %s", quote_identifier(root->path->name));

    // Handle PASSING clause if present
    if (jexpr->passing_values) {
        bool needcomma = false;

        appendStringInfoChar(buf, ' ');
        appendContextKeyword(context, "PASSING ", 0, 0, 0);

        if (PRETTY_INDENT(context))
            context->indentLevel += PRETTYINDENT_VAR;

        forboth(lc1, jexpr->passing_names, lc2, jexpr->passing_values) {
            if (needcomma)
                appendStringInfoString(buf, ", ");
            needcomma = true;

            appendContextKeyword(context, "", 0, 0, 0);
            get_rule_expr((Node *) lfirst(lc2), context, false);
            appendStringInfo(buf, " AS %s",
                quote_identifier((lfirst_node(String, lc1))->sval));
        }

        if (PRETTY_INDENT(context))
            context->indentLevel -= PRETTYINDENT_VAR;
    }

    // Output column specifications
    get_json_table_columns(tf, castNode(JsonTablePathScan, tf->plan), context, showimplicit);

    // Handle error behavior if not default
    if (jexpr->on_error->btype != JSON_BEHAVIOR_EMPTY_ARRAY)
        get_json_behavior(jexpr->on_error, context, "ERROR");

    if (PRETTY_INDENT(context))
        context->indentLevel -= PRETTYINDENT_VAR;

    appendContextKeyword(context, ")", 0, 0, 0);
}
```