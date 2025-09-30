# get_json_table_columns

## Location
[src/backend/utils/adt/ruleutils.c:11746-11851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11746-L11851)

## Overview
Formats and outputs the column specifications for JSON_TABLE expressions during SQL query deparsing, handling various column types including ordinality, exists, and query operations.

## Definition
```c
static void get_json_table_columns(TableFunc *tf, JsonTablePathScan *scan,
                                   deparse_context *context,
                                   bool showimplicit)
```

## Detailed Description
This function is responsible for reconstructing the COLUMNS clause of JSON_TABLE expressions from their internal representation. It iterates through all columns defined in the TableFunc and formats them according to their types and specifications. The function handles:

1. **Column filtering**: Only processes columns within the scan range (colMin to colMax)
2. **Multiple column types**: Regular columns, ordinality columns, EXISTS columns, and QUERY columns
3. **Type formatting**: Applies appropriate type specifications and modifiers
4. **JSON-specific syntax**: Handles FORMAT JSON/JSONB, PATH specifications, and behavior options
5. **Nested structures**: Recursively processes child scans for nested column specifications
6. **Pretty printing**: Supports indentation and formatting for readable output

The function properly formats SQL syntax including comma separation, proper quoting of identifiers, and context-aware keyword placement.

## Parameters / Member Variables
- `tf`: TableFunc structure containing the complete table function definition including column names, types, and expressions
- `scan`: JsonTablePathScan that defines the specific range of columns to process (colMin to colMax)
- `context`: deparse_context containing the output buffer, indentation level, and formatting preferences
- `showimplicit`: Boolean flag indicating whether to display implicit path specifications

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoChar](../a/appendStringInfoChar.md), appendStringInfoString, appendStringInfo
  - [appendContextKeyword](../a/appendContextKeyword.md)
  - PRETTY_INDENT, PRETTYINDENT_VAR (formatting macros)
  - forfour (macro for iterating over four parallel lists)
  - strVal, lfirst, lfirst_oid, lfirst_int (list manipulation macros)
  - castNode (safe type casting macro)
  - [quote_identifier](../q/quote_identifier.md)
  - [format_type_with_typemod](../f/format_type_with_typemod.md)
  - [get_type_category_preferred](get_type_category_preferred.md)
  - [get_json_path_spec](get_json_path_spec.md)
  - [get_json_expr_options](get_json_expr_options.md)
  - [get_json_table_nested_columns](get_json_table_nested_columns.md) (for child scans)
- Called from (representative examples):
  - [get_json_table_nested_columns](get_json_table_nested_columns.md)
  - [get_json_table](get_json_table.md)

## Notes and Other Information
- This is a static function used internally by the rule deparsing system
- The function handles different JSON operation types (EXISTS, QUERY, VALUE) with appropriate syntax
- Column range filtering allows for partial column processing in nested structures
- FORMAT specifications are only added for string-category types in JSON_QUERY operations
- The function maintains proper SQL syntax compliance when reconstructing complex JSON_TABLE expressions
- Part of PostgreSQL's JSON_TABLE feature for converting JSON data to relational format

## Simplified Source

```c
static void get_json_table_columns(TableFunc *tf, JsonTablePathScan *scan,
                                   deparse_context *context, bool showimplicit) {
    StringInfo buf = context->buf;
    int colnum = 0;

    // Start COLUMNS clause
    appendStringInfoChar(buf, ' ');
    appendContextKeyword(context, "COLUMNS (", 0, 0, 0);

    // Iterate through all columns in parallel lists
    forfour(lc_colname, tf->colnames,
            lc_coltype, tf->coltypes,
            lc_coltypmod, tf->coltypmods,
            lc_colvalexpr, tf->colvalexprs) {

        char *colname = strVal(lfirst(lc_colname));
        JsonExpr *colexpr = castNode(JsonExpr, lfirst(lc_colvalexpr));
        Oid typid = lfirst_oid(lc_coltype);
        int32 typmod = lfirst_int(lc_coltypmod);

        // Skip columns outside scan range
        if (scan->colMin >= 0 && (colnum < scan->colMin || colnum > scan->colMax)) {
            colnum++;
            continue;
        }

        // Add comma separator for multiple columns
        if (colnum > scan->colMin)
            appendStringInfoString(buf, ", ");
        colnum++;

        // Handle ordinality columns (no expression)
        bool ordinality = !colexpr;
        appendStringInfo(buf, "%s %s", quote_identifier(colname),
                        ordinality ? "FOR ORDINALITY" :
                        format_type_with_typemod(typid, typmod));

        if (ordinality)
            continue;

        // Handle different JSON operation types
        JsonBehaviorType default_behavior;
        if (colexpr->op == JSON_EXISTS_OP) {
            appendStringInfoString(buf, " EXISTS");
            default_behavior = JSON_BEHAVIOR_FALSE;
        } else {
            // Handle JSON_QUERY operations with format specifications
            if (colexpr->op == JSON_QUERY_OP) {
                char typcategory;
                bool typispreferred;
                get_type_category_preferred(typid, &typcategory, &typispreferred);

                if (typcategory == TYPCATEGORY_STRING) {
                    appendStringInfoString(buf,
                        colexpr->format->format_type == JS_FORMAT_JSONB ?
                        " FORMAT JSONB" : " FORMAT JSON");
                }
            }
            default_behavior = JSON_BEHAVIOR_NULL;
        }

        // Add PATH specification and options
        appendStringInfoString(buf, " PATH ");
        get_json_path_spec(colexpr->path_spec, context, showimplicit);
        get_json_expr_options(colexpr, context, default_behavior);
    }

    // Handle nested columns
    if (scan->child)
        get_json_table_nested_columns(tf, scan->child, context, showimplicit,
                                      scan->colMin >= 0);

    appendContextKeyword(context, ")", 0, 0, 0);
}
```