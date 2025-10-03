# ExplainPropertyText

## Location
[src/backend/commands/explain.c:4802-4810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4802-L4810)

## Overview
Public wrapper function for displaying string-valued properties in EXPLAIN output, providing a simplified interface to the core ExplainProperty function.

## Definition

```c
void
ExplainPropertyText(const char *qlabel, const char *value, ExplainState *es)
```
## Detailed Description
This is a convenience function that serves as a type-specific wrapper around the core ExplainProperty function. It is specifically designed for displaying text/string properties in EXPLAIN output. The function automatically sets appropriate parameters for string handling:

- Sets the unit parameter to NULL (no units for text properties)
- Sets the numeric flag to false, ensuring the value will be properly quoted and escaped in JSON/YAML formats
- Delegates all actual formatting work to the underlying ExplainProperty function

This function provides a clean, type-safe interface for the most common use case of displaying string properties, eliminating the need for callers to remember the correct parameter combinations for text values.

## Parameters / Member Variables
- `*qlabel`: The property label/name to be displayed
- `*value`: The string value to be displayed
- `*es`: Pointer to ExplainState containing output format information and string buffer
## Dependencies
- Functions called/Symbols referenced:
  - [ExplainState](ExplainState.md) (struct type)
  - [ExplainProperty](ExplainProperty.md)
- Called from (representative examples):
  - [ExplainPrintSettings](ExplainPrintSettings.md) (at src/backend/commands/explain.c:830, 861)
  - [ExplainPrintSerialize](ExplainPrintSerialize.md) (at src/backend/commands/explain.c:1152)
  - [ExplainQueryText](ExplainQueryText.md) (at src/backend/commands/explain.c:1172)
  - [ExplainQueryParameters](ExplainQueryParameters.md) (at src/backend/commands/explain.c:1194)
  - [report_triggers](../r/report_triggers.md) (at src/backend/commands/explain.c:1254, 1256, 1257)
  - [ExplainNode](ExplainNode.md) (at src/backend/commands/explain.c:1654, 1656, 1658, 1660, 1662, 1664, 1666, 1721, 1772, 1800)
  - [show_expression](../s/show_expression.md) (at src/backend/commands/explain.c:2503)
  - [show_grouping_set_keys](../s/show_grouping_set_keys.md) (at src/backend/commands/explain.c:2722)
  - [show_tablesample](../s/show_tablesample.md) (at src/backend/commands/explain.c:2934, 2937)
  - [show_sort_info](../s/show_sort_info.md) (at src/backend/commands/explain.c:2971, 2973, 3016, 3018)
  - [show_memoize_info](../s/show_memoize_info.md) (at src/backend/commands/explain.c:3363, 3364)
  - [ExplainIndexScanDetails](ExplainIndexScanDetails.md) (at src/backend/commands/explain.c:4003, 4004)
  - [ExplainTargetRel](ExplainTargetRel.md) (at src/backend/commands/explain.c:4156, 4158, 4159)
  - [show_modifytable_info](../s/show_modifytable_info.md) (at src/backend/commands/explain.c:4284)

## Notes and Other Information
- This is a public function (not static), widely available throughout the PostgreSQL codebase
- One of the most frequently used EXPLAIN property functions, as evidenced by its extensive usage throughout the explain system
- Part of a family of type-specific wrapper functions including ExplainPropertyInteger, ExplainPropertyFloat, ExplainPropertyBool, etc.
- Automatically handles proper escaping and quoting for string values across all output formats
- The function is simple but essential, providing type safety and API consistency for string property display
- Used extensively for displaying node types, relation names, index names, expressions, and other textual information in query plans

## Simplified Source

```c
void
ExplainPropertyText(const char *qlabel, const char *value, ExplainState *es)
{
    // Delegate to generic property function with string-specific parameters
    ExplainProperty(qlabel, NULL, value, false, es);
}
```