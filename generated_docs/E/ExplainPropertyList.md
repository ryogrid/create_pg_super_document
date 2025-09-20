# ExplainPropertyList

## Location
[src/backend/commands/explain.c:4626-4695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L4626-L4695)

## Overview
Formats and outputs a property that consists of a list of unlabeled items (such as sort keys or targets) in the appropriate format based on the current explain output mode.

## Definition

```c
void
ExplainPropertyList(const char *qlabel, List *data, ExplainState *es)
```
## Detailed Description
This function handles the formatting of list-type properties in EXPLAIN output across all supported output formats (TEXT, XML, JSON, YAML). It takes a list of C strings and formats them appropriately for each output mode:

- **TEXT format**: Creates a comma-separated list on a single line with proper indentation
- **XML format**: Wraps items in  tags within a parent tag named after the property, with proper XML escaping
- **JSON format**: Creates a JSON array with proper escaping and comma separation
- **YAML format**: Creates a YAML list with proper indentation and dash formatting

The function ensures proper escaping for each format to prevent injection attacks and maintain valid output structure.

## Parameters / Member Variables
- : The label/name for the property being displayed
- : A PostgreSQL List containing C strings to be formatted and output
- : Pointer to ExplainState containing output format information and string buffer

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (struct type)
  - EXPLAIN_FORMAT_TEXT, EXPLAIN_FORMAT_XML, EXPLAIN_FORMAT_JSON, EXPLAIN_FORMAT_YAML (enum values)
  - [ExplainIndentText](ExplainIndentText.md)
  - [ExplainXMLTag](ExplainXMLTag.md)
  - [ExplainJSONLineEnding](ExplainJSONLineEnding.md)
  - [ExplainYAMLLineStarting](ExplainYAMLLineStarting.md)
  - appendStringInfo, appendStringInfoString, appendStringInfoChar, appendStringInfoSpaces
  - [escape_xml](../e/escape_xml.md), escape_json, escape_yaml
  - lfirst (PostgreSQL list macro)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [show_plan_tlist](../s/show_plan_tlist.md) (at src/backend/commands/explain.c:2480)
  - [show_sort_group_keys](../s/show_sort_group_keys.md) (at src/backend/commands/explain.c:2811, 2813)
  - [show_tablesample](../s/show_tablesample.md) (at src/backend/commands/explain.c:2935)
  - [show_incremental_sort_group_info](../s/show_incremental_sort_group_info.md) (at src/backend/commands/explain.c:3105)
  - [show_modifytable_info](../s/show_modifytable_info.md) (at src/backend/commands/explain.c:4294)
  - [ExplainPropertyListNested](ExplainPropertyListNested.md) (at src/backend/commands/explain.c:4705)

## Notes and Other Information
- This is a public function (not static), available for use throughout the PostgreSQL codebase
- The function handles empty lists gracefully across all formats
- Memory management includes freeing escaped XML strings but not the input data list
- The function maintains proper indentation and formatting consistency across all output modes
- Each output format has specific escaping requirements that are properly handled
- Used extensively throughout the explain system for displaying various types of list data like sort keys, target lists, and other array-like properties