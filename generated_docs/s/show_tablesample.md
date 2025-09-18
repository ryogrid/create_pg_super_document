# show_tablesample

## Location
src/backend/commands/explain.c: 2879 - 2944

## Overview
A static function that displays TABLESAMPLE clause information in PostgreSQL's EXPLAIN output, showing the sampling method, parameters, and optional repeatable seed.

## Definition
```c
static void
show_tablesample(TableSampleClause *tsc, PlanState *planstate,
                 List *ancestors, ExplainState *es)
```

## Detailed Description
The `show_tablesample` function formats and displays information about table sampling operations in PostgreSQL's EXPLAIN output. Table sampling allows users to retrieve a random subset of rows from a table using various sampling methods. This function extracts the sampling method name, deparses parameter expressions, handles the optional REPEATABLE clause, and formats the output appropriately for both text and non-text (JSON, XML, YAML) EXPLAIN formats.

The function works by looking up the sampling method name from the handler function, deparasing all parameter expressions to human-readable form, and then formatting the output according to the selected EXPLAIN format. For text format, it creates a readable "Sampling:" line, while for structured formats it uses separate properties for method, parameters, and repeatable seed.

## Parameters / Member Variables
- `tsc`: Pointer to the TableSampleClause containing sampling information
- `planstate`: The plan state context for expression deparsing
- `ancestors`: List of ancestor plan nodes for context during deparsing
- `es`: ExplainState containing output formatting options

## Dependencies
- Functions called/Symbols referenced:
  - [set_deparse_context_plan](set_deparse_context_plan.md) (sets up context for expression deparsing)
  - [get_func_name](../g/get_func_name.md) (converts function OID to name for the sampling method)
  - [deparse_expression](../d/deparse_expression.md) (converts expression nodes to readable text)
  - [ExplainIndentText](../E/ExplainIndentText.md) (adds proper indentation in text format)
  - [ExplainPropertyText](../E/ExplainPropertyText.md) (outputs named text properties in structured formats)
  - [ExplainPropertyList](../E/ExplainPropertyList.md) (outputs named list properties in structured formats)
  - appendStringInfo/appendStringInfoString/appendStringInfoChar (builds output string)
- Constants referenced:
  - EXPLAIN_FORMAT_TEXT (indicates text output format)
- Types referenced:
  - [TableSampleClause](../T/TableSampleClause.md), PlanState, ExplainState, ListCell
- Called from (representative examples):
  - [ExplainNode](../E/ExplainNode.md) (when explaining nodes that use table sampling)

## Notes and Other Information
- Handles both text and structured output formats differently for optimal readability
- The REPEATABLE clause is optional and only displayed when present
- Parameters are comma-separated in text format but presented as a list in structured formats
- Uses proper expression deparsing to handle complex parameter expressions
- Determines table prefixing based on whether multiple tables are involved in the query
- The sampling method name is derived from the table sampling method handler function
- Text format output follows the SQL TABLESAMPLE syntax: "Sampling: method_name (param1, param2) REPEATABLE (seed)"
- Structured format separates components into distinct properties: "Sampling Method", "Sampling Parameters", and "Repeatable Seed"