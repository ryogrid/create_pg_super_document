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